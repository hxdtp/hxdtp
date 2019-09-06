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
	defaultTimeout           = 5 * time.Second
	defaultServerReadTimeout = 30 * time.Second
)

var (
	ErrServerAlreadyStarted = fmt.Errorf("hxdtp: server already started")
	ErrServerClosed         = fmt.Errorf("hxdtp: server closed")
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
	Parallel        bool
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	GracefulTimeout time.Duration
}

func (conf *ServerConfig) withDefaults() {
	if conf.ReadTimeout <= 0 {
		conf.ReadTimeout = defaultServerReadTimeout
	}
	if conf.WriteTimeout <= 0 {
		conf.WriteTimeout = defaultTimeout
	}
	if conf.GracefulTimeout <= 0 {
		conf.GracefulTimeout = defaultTimeout
	}
}

type Server struct {
	ctx        context.Context
	cancel     context.CancelFunc
	conf       ServerConfig
	handleFunc HandleFunc
	pool       *serverContextPool

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
		return nil, fmt.Errorf("hxdtp: HandleFunc required")
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
		pool:       newServerContextPool(),
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

	proto, err := protocol.Select(newBufferedTransport(conn))
	if err != nil {
		panic("TODO")
	}

	if s.conf.Parallel {
		s.handleRequestsParallel(conn, proto)
	} else {
		s.handleRequestsSequential(conn, proto)
	}
}

func (s *Server) handleRequestsSequential(conn net.Conn, proto protocol.VersionedProtocol) {
	for {
		func() {
			if err := conn.SetDeadline(time.Now().Add(s.conf.ReadTimeout)); err != nil {
				panic(err)
			}
			msg, err := proto.ReadMessage()
			if err != nil {
				// TODO: hook for logging/metrics...
				return
			}
			// TODO
			svrctx := s.pool.Get()
			defer s.pool.Put(svrctx)
			svrctx.Reset(s.ctx, msg, proto.NewMessageFrom(msg))
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
	// TODO
	panic("TODO")
}

type serverContext struct {
	stdctx context.Context
	cancel context.CancelFunc

	ml   sync.RWMutex
	meta map[string]interface{}

	request  serverRequest
	response *serverResponse
}

func newServerContext() *serverContext {
	return &serverContext{
		meta:     map[string]interface{}{},
		response: &serverResponse{},
	}
}

func (sctx *serverContext) Reset(stdctx context.Context, req, resp protocol.Message) {
	if sctx.cancel != nil {
		sctx.cancel()
	}
	if stdctx == nil {
		sctx.stdctx = nil
		sctx.cancel = nil
	} else {
		subctx, cancel := context.WithCancel(stdctx)
		sctx.stdctx = subctx
		sctx.cancel = cancel
	}
	sctx.request.Message = req
	sctx.response.reset(resp)
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

type serverContextPool struct {
	pool sync.Pool
}

func newServerContextPool() *serverContextPool {
	return &serverContextPool{
		pool: sync.Pool{
			New: func() interface{} {
				return newServerContext()
			},
		},
	}
}

func (p *serverContextPool) Get() *serverContext {
	return p.pool.Get().(*serverContext)
}

func (p *serverContextPool) Put(ctx *serverContext) {
	ctx.Reset(nil, nil, nil)
	p.pool.Put(ctx)
}
