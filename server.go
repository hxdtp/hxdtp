package hxdtp

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/hxdtp/hxdtp/protocol"
)

const (
	optionMultiplex = "m"

	defaultReadTimeout       = 3 * time.Second
	defaultWriteTimeout      = 100 * time.Millisecond
	defaultServerReadTimeout = 30 * time.Second
)

var (
	ErrServerAlreadyStarted = fmt.Errorf("hxdtp: server already started")
	ErrServerClosed         = fmt.Errorf("hxdtp: server closed")

	newServerResponse = newWriteableMessage
	newServerRequst   = newReadonlyMessage
)

type (
	ServerRequest  readonlyMessager
	ServerResponse writeableMessager
	serverRequest  = readonlyMessage
	serverResponse = writeableMessage
	ServerContext  interface {
		Ctx() context.Context
		Set(key string, value interface{})
		Get(key string) interface{}
		Request() ServerRequest
		Response() ServerResponse
	}
)

type ServerConfig struct {
	HandleFunc      HandleFunc
	Middlewares     []HandleMiddleware
	Concurrency     int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	GracefulTimeout time.Duration
}

func (conf *ServerConfig) withDefaults() {
	if conf.ReadTimeout <= 0 {
		conf.ReadTimeout = defaultServerReadTimeout
	}
	if conf.WriteTimeout <= 0 {
		conf.WriteTimeout = defaultWriteTimeout
	}
	if conf.GracefulTimeout <= 0 {
		conf.GracefulTimeout = 5 * time.Second
	}
}

// TODO: test and optimize the concurrency model.

type Server struct {
	ctx        context.Context
	cancel     context.CancelFunc
	conf       ServerConfig
	handleFunc HandleFunc
	pools      *serverContextPools

	l       net.Listener
	wg      sync.WaitGroup
	started int32
	closed  int32
}

func NewServer(laddr string, conf ServerConfig) (*Server, error) {
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return NewServerWithListener(l, conf)
}

func NewServerWithListener(l net.Listener, conf ServerConfig) (*Server, error) {
	if conf.HandleFunc == nil {
		return nil, errors.Errorf("hxdtp: HandleFunc required")
	}
	(&conf).withDefaults()
	handleFunc := conf.HandleFunc
	for i := len(conf.Middlewares) - 1; i >= 0; i-- {
		handleFunc = conf.Middlewares[i](handleFunc)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:        ctx,
		cancel:     cancel,
		conf:       conf,
		handleFunc: handleFunc,
		pools:      newServerContextPools(),
		l:          l,
	}
	go func() {
		<-ctx.Done()
		s.Close()
	}()
	return s, nil
}

func (s *Server) Serve() error {
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		return ErrServerAlreadyStarted
	}

	for {
		if atomic.LoadInt32(&s.closed) == 1 {
			return ErrServerClosed
		}

		conn, err := s.l.Accept()
		if err != nil {
			return errors.WithStack(err)
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	s.cancel()
	err := s.l.Close()

	donec := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(donec)
	}()
	select {
	case <-time.After(s.conf.GracefulTimeout):
	case <-donec:
	}
	return errors.WithStack(err)
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic: %v: %v\n", e, string(debug.Stack()))
		}
		conn.Close()
		s.wg.Done()
	}()

	// TODO(damnever): resuse transport
	proto, err := protocol.SelectProtocol(newBufferedTransport(conn))
	if err != nil {
		panic("TODO")
	}

	svrctx := s.pools.Get(proto.Version())
	defer s.pools.Put(svrctx)
	if err = conn.SetDeadline(time.Now().Add(s.conf.ReadTimeout)); err != nil {
		panic(err)
	}
	reqmsg := svrctx.RawRequestMessage()
	if err = proto.ReadMessage(reqmsg); err != nil {
		return
	}
	respmsg := svrctx.RawResponseMessage()
	respmsg.SetSeqID(reqmsg.SeqID())
	multiplex := false
	if v, ok := reqmsg.Headers().Get(optionMultiplex).(int64); ok && v == 1 {
		multiplex = true
		respmsg.Headers().Set(optionMultiplex, 1)
	} else {
		respmsg.Headers().Set(optionMultiplex, 0)
	}
	if err = conn.SetWriteDeadline(time.Now().Add(s.conf.WriteTimeout)); err != nil {
		panic(err)
	}
	if err = proto.WriteMessage(respmsg); err != nil {
		panic(err)
	}
	if multiplex {
		s.handleRequestsParallel(conn, proto)
	} else {
		s.handleRequestsSequential(conn, proto)
	}
}

func (s *Server) handleRequestsSequential(conn net.Conn, proto protocol.VersionedProtocol) {
	version := proto.Version()
	for {
		func() {
			if err := conn.SetDeadline(time.Now().Add(s.conf.ReadTimeout)); err != nil {
				panic(err)
			}
			svrctx := s.pools.Get(version)
			svrctx.WithCtx(s.ctx)
			defer s.pools.Put(svrctx)
			reqmsg := svrctx.RawRequestMessage()
			if err := proto.ReadMessage(reqmsg); err != nil {
				// TODO: hook for logging/metrics...
				return
			}
			// TODO
			svrctx.RawResponseMessage().SetSeqID(reqmsg.SeqID())
			if err := s.handleFunc(svrctx); err != nil {
				panic(err)
			}
			if err := conn.SetWriteDeadline(time.Now().Add(s.conf.WriteTimeout)); err != nil {
				panic(err)
			}
			if err := proto.WriteMessage(svrctx.response.tomsg()); err != nil {
				panic(err)
			}
		}()
		if atomic.LoadInt32(&s.closed) == 1 {
			return
		}
	}
}

func (s *Server) handleRequestsParallel(conn net.Conn, proto protocol.VersionedProtocol) {
	version := proto.Version()
	rproto := proto
	wproto, err := protocol.NewProtocol(version, newBufferedTransport(conn))
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}
	respc := make(chan *serverContext)
	defer func() {
		wg.Wait()
		close(respc)
	}()
	go func() {
		var err error
		// TODO(damnever)
		for svrctx := range respc {
			if err != nil {
				goto RELEASE_CTX
			}
			if err = conn.SetWriteDeadline(time.Now().Add(s.conf.WriteTimeout)); err != nil {
				// TODO
				goto RELEASE_CTX
			}
			if err = wproto.WriteMessage(svrctx.response.tomsg()); err != nil {
				// TODO
				goto RELEASE_CTX
			}
		RELEASE_CTX:
			s.pools.Put(svrctx)
		}
	}()

	for {
		if err := conn.SetDeadline(time.Now().Add(s.conf.ReadTimeout)); err != nil {
			panic(err)
		}
		svrctx := s.pools.Get(version)
		svrctx.WithCtx(s.ctx)
		reqmsg := svrctx.RawRequestMessage()
		if err := rproto.ReadMessage(reqmsg); err != nil {
			// TODO: put context back & hook for logging/metrics...
			return
		}
		svrctx.RawResponseMessage().SetSeqID(reqmsg.SeqID())
		// TODO: pooling
		wg.Add(1)
		go func(svrctx *serverContext) {
			defer wg.Done()
			if err := s.handleFunc(svrctx); err != nil {
				panic(err)
			}
			select {
			case respc <- svrctx:
			case <-s.ctx.Done():
			}
		}(svrctx)
		if atomic.LoadInt32(&s.closed) == 1 {
			return
		}
	}
}

type serverContext struct {
	stdctx context.Context
	cancel context.CancelFunc

	ml   sync.RWMutex
	meta map[string]interface{}

	version  protocol.Version
	request  *serverRequest
	response *serverResponse
}

func newServerContext(version protocol.Version) *serverContext {
	reqmsg, err := protocol.NewMessage(version)
	if err != nil { // Since the protocol already registered
		panic(err)
	}
	respmsg, err := protocol.NewMessage(version)
	if err != nil {
		panic(err)
	}
	return &serverContext{
		meta:     map[string]interface{}{},
		request:  newServerRequst(reqmsg),
		response: newServerResponse(respmsg),
	}
}

func (sctx *serverContext) Version() protocol.Version {
	return sctx.version
}

func (sctx *serverContext) RawRequestMessage() protocol.Message {
	return sctx.request.Message
}

func (sctx *serverContext) RawResponseMessage() protocol.Message {
	return sctx.response.Message
}

func (sctx *serverContext) WithCtx(stdctx context.Context) {
	subctx, cancel := context.WithCancel(stdctx)
	sctx.stdctx = subctx
	sctx.cancel = cancel
}

func (sctx *serverContext) Reset() {
	if sctx.cancel != nil {
		sctx.cancel()
	}
	sctx.stdctx = nil
	sctx.cancel = nil
	sctx.request.reset()
	sctx.response.reset()
	for k := range sctx.meta {
		delete(sctx.meta, k)
	}
}

func (sctx *serverContext) Ctx() context.Context {
	return sctx.stdctx
}

func (sctx *serverContext) Request() ServerRequest {
	return sctx.request
}

func (sctx *serverContext) Response() ServerResponse {
	return sctx.response
}

func (sctx *serverContext) Set(key string, value interface{}) {
	sctx.ml.Lock()
	sctx.meta[key] = value
	sctx.ml.Unlock()
}

func (sctx *serverContext) Get(key string) interface{} {
	sctx.ml.RLock()
	val := sctx.meta[key]
	sctx.ml.RUnlock()
	return val
}

type serverContextPools struct {
	l     sync.RWMutex
	pools map[protocol.Version]*sync.Pool
}

func newServerContextPools() *serverContextPools {
	return &serverContextPools{
		pools: map[protocol.Version]*sync.Pool{},
	}
}

func (ps *serverContextPools) Get(version protocol.Version) *serverContext {
	ps.l.RLock()
	p, ok := ps.pools[version]
	ps.l.RUnlock()
	if ok {
		return p.Get().(*serverContext)
	}

	ps.l.Lock()
	defer ps.l.Unlock()
	if p, ok = ps.pools[version]; ok {
		return p.Get().(*serverContext)
	}
	p = &sync.Pool{
		New: func() interface{} {
			return newServerContext(version)
		},
	}
	ps.pools[version] = p
	return p.Get().(*serverContext)
}

func (ps *serverContextPools) Put(ctx *serverContext) {
	ctx.Reset()
	ps.l.Lock()
	if p, ok := ps.pools[ctx.Version()]; ok {
		p.Put(ctx)
	}
	ps.l.Unlock()
}
