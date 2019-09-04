package hxdtp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hxdtp/hxdtp/protocol"
)

type (
	ServerRequest interface {
		// TODO: method to get all kvs by prefix
		Get(key string) interface{}
		Body() io.Reader
	}
	ServerResponse interface {
		Set(key string, value interface{}) ServerResponse
		WithBlob(p []byte) ServerResponse
		WithStream(rd io.LimitedReader) ServerResponse
		WithFile(fsr FileSender) ServerResponse
	}
	ServerContext interface {
		Ctx() context.Context
		Set(key string, value interface{})
		Get(key string) interface{}
		Request() ServerRequest
		Response() ServerResponse
	}
)

type ServerOptions struct {
	ListenAddr      string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	GracefulTimeout time.Duration
	HandleFunc      HandleFunc
	Parallel        bool
}

type Server struct {
	ctx  context.Context
	opts ServerOptions
	pool *serverContextPool

	l      net.Listener
	wg     sync.WaitGroup
	closed int32
}

func NewServer(ctx context.Context, opts ServerOptions) (*Server, error) {
	l, err := net.Listen("tcp", opts.ListenAddr)
	if err != nil {
		return nil, err
	}
	// TODO(damnever): check if context canceled
	return &Server{
		ctx:  ctx,
		opts: opts,
		pool: newServerContextPool(),
		l:    l,
	}, nil
}

// TODO(damnever): NewServerWithListener

func (s *Server) Serve() error {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return err
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	err := s.l.Close()
	donec := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(donec)
	}()
	select {
	case <-time.After(s.opts.GracefulTimeout):
	case <-donec:
	}
	return err
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

	if s.opts.Parallel {
		s.handleRequestsParallel(conn, proto)
	} else {
		s.handleRequestsSequential(conn, proto)
	}
}

func (s *Server) handleRequestsSequential(conn net.Conn, proto protocol.VersionedProtocol) {
	for {
		func() {
			if err := conn.SetDeadline(time.Now().Add(s.opts.ReadTimeout)); err != nil {
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
			if err := s.opts.HandleFunc(svrctx); err != nil {
				panic(err)
			}
			if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout)); err != nil {
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
}

type serverRequest struct {
	protocol.Message
}

func (r serverRequest) Get(key string) interface{} {
	return r.Message.Headers().Get(key)
}

func (r serverRequest) Body() io.Reader {
	return r.Message.Body().R
}

type serverResponse struct {
	protocol.Message
	rdc *readerChain
}

func (r *serverResponse) reset(msg protocol.Message) {
	r.Message = msg
	if r.rdc == nil {
		r.rdc = &readerChain{}
	}
	r.rdc.Reset()
}

func (r *serverResponse) tomsg() protocol.Message {
	r.Message.SetBody(io.LimitedReader{
		R: r.rdc,
		N: r.rdc.size,
	})
	return r.Message
}

func (r *serverResponse) Set(key string, value interface{}) ServerResponse {
	r.Message.Headers().Set(key, value)
	return r
}

func (r *serverResponse) WithBlob(p []byte) ServerResponse {
	r.rdc.Append(bytes.NewBuffer(p), int64(len(p)))
	return r
}

func (r *serverResponse) WithStream(rd io.LimitedReader) ServerResponse {
	r.rdc.Append(rd.R, rd.N)
	return r
}

func (r *serverResponse) WithFile(fsr FileSender) ServerResponse {
	r.rdc.Append(fileSender{fsr}, fsr.Count())
	return r
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
