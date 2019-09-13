package hxdtp

import (
	"context"
	"fmt"
	"math"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/hxdtp/hxdtp/protocol"
)

type (
	ClientRequest  writeableMessager
	ClientResponse readonlyMessager
	clientRequest  = writeableMessage
	clientResponse = readonlyMessage
)

var (
	ErrClientClosed   = fmt.Errorf("hxdtp: client closed")
	ErrSeqIDExhausted = fmt.Errorf("hxdtp: sequence id exhausted")
)

type ClientConfig struct {
	ProtoVersion protocol.Version
	Multiplex    bool
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Middlewares  []CallMiddleware
}

func (conf *ClientConfig) withDefaults() {
	if conf.DialTimeout <= 0 {
		conf.DialTimeout = time.Second
	}
	if conf.ReadTimeout <= 0 {
		conf.ReadTimeout = defaultReadTimeout
	}
	if conf.WriteTimeout <= 0 {
		conf.WriteTimeout = defaultWriteTimeout
	}
}

type Client struct {
	conf     ClientConfig
	callFunc CallFunc

	seqid uint64
	conn  net.Conn
	proto protocol.VersionedProtocol
}

func NewClient(raddr string, conf ClientConfig) (*Client, error) {
	dialer := net.Dialer{Timeout: conf.DialTimeout}
	conn, err := dialer.Dial("tcp", raddr)
	if err != nil {
		return nil, err
	}
	return NewClientWithConn(conn, conf)
}

func NewClientWithConn(conn net.Conn, conf ClientConfig) (*Client, error) {
	if !protocol.Registered(conf.ProtoVersion) {
		return nil, errors.Errorf("hxdtp: protocol version not registered")
	}
	(&conf).withDefaults()
	// TODO(damnever): reuse transport
	proto, err := protocol.Connect(conf.ProtoVersion, newBufferedTransport(conn))
	if err != nil {
		conn.Close()
		return nil, err
	}

	c := &Client{
		conf:  conf,
		seqid: 0,
		conn:  conn,
		proto: proto,
	}
	c.callFunc = c.call
	for i := len(conf.Middlewares) - 1; i >= 0; i-- {
		c.callFunc = conf.Middlewares[i](c.callFunc)
	}

	if err := c.negotiate(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Client) negotiate() error {
	req, err := c.NewRequest()
	if err != nil {
		return err
	}
	resp, err := c.callFunc(context.TODO(), req.Set(optionMultiplex, 0))
	if err != nil {
		return err
	}
	if v, ok := resp.Get(optionMultiplex).(int64); ok && v == 1 {
		return errors.Errorf("hxdtp: server DOES NOT support non-multiplexing model")
	}
	return nil
}

func (c *Client) NewRequest() (ClientRequest, error) {
	clireq := c.proto.NewMessage()
	c.seqid++
	if c.seqid >= math.MaxUint64 {
		c.conn.Close()
		return nil, ErrSeqIDExhausted
	}
	clireq.SetSeqID(c.seqid)
	return newClientRequst(clireq), nil
}

func (c *Client) Call(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	return c.callFunc(ctx, req)
}

func (c *Client) call(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	clireq, ok := req.(*clientRequest)
	if !ok {
		return nil, errors.Errorf("hxdtp: unknown request type: %s", reflect.TypeOf(req))
	}

	if err := c.conn.SetWriteDeadline(deadline(ctx, c.conf.WriteTimeout)); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := c.proto.WriteMessage(clireq.tomsg()); err != nil {
		return nil, err
	}
	if err := c.conn.SetReadDeadline(deadline(ctx, c.conf.ReadTimeout)); err != nil {
		return nil, errors.WithStack(err)
	}
	cliresp, err := c.proto.ReadMessage()
	if err != nil {
		return nil, err
	}
	if cliresp.SeqID() != clireq.SeqID() {
		panic("hxdtp: seqid mismatch, non-multiplex client can not be called concurrently")
	}
	return clientResponse{cliresp}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type MultiplexClient struct {
	conf     ClientConfig
	callFunc CallFunc

	seqid uint64
	conn  net.Conn
	pl    sync.Mutex
	proto protocol.VersionedProtocol

	regc   chan *clientSeqIDWithResponse
	deregc chan uint64
	respc  chan clientResponse
	closed int32
	stopc  chan struct{}
	donec  chan struct{}
}

func NewMultiplexClient(raddr string, conf ClientConfig) (*MultiplexClient, error) {
	dialer := net.Dialer{Timeout: conf.DialTimeout}
	conn, err := dialer.Dial("tcp", raddr)
	if err != nil {
		return nil, err
	}
	return NewMultiplexClientWithConn(conn, conf)
}

func NewMultiplexClientWithConn(conn net.Conn, conf ClientConfig) (*MultiplexClient, error) {
	if !protocol.Registered(conf.ProtoVersion) {
		return nil, errors.Errorf("hxdtp: protocol version not registered")
	}
	(&conf).withDefaults()
	proto, err := protocol.Connect(conf.ProtoVersion, newBufferedTransport(conn))
	if err != nil {
		conn.Close()
		return nil, err
	}

	c := &MultiplexClient{
		conf:   conf,
		seqid:  0,
		conn:   conn,
		proto:  proto,
		regc:   make(chan *clientSeqIDWithResponse),
		deregc: make(chan uint64),
		respc:  make(chan clientResponse),
		closed: 0,
		stopc:  make(chan struct{}),
		donec:  make(chan struct{}),
	}
	c.callFunc = c.call
	for i := len(conf.Middlewares) - 1; i >= 0; i-- {
		c.callFunc = conf.Middlewares[i](c.callFunc)
	}
	if err := c.negotiate(); err != nil {
		c.Close()
		return nil, err
	}
	go c.loop()
	return c, nil
}

func (c *MultiplexClient) negotiate() error {
	req := c.proto.NewMessage()
	req.SetSeqID(atomic.AddUint64(&c.seqid, 1))
	req.Headers().Set(optionMultiplex, 1)

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.conf.WriteTimeout)); err != nil {
		return errors.WithStack(err)
	}
	if err := c.proto.WriteMessage(req); err != nil {
		return err
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(c.conf.ReadTimeout)); err != nil {
		return errors.WithStack(err)
	}
	resp, err := c.proto.ReadMessage()
	if err != nil {
		return err
	}
	if resp.SeqID() != req.SeqID() {
		panic("hxdtp: seqid mismatch, non-multiplex client can not be called concurrently")
	}
	if v, ok := resp.Headers().Get(optionMultiplex).(int64); !ok || v == 0 {
		return errors.Errorf("hxdtp: server DOES NOT support multiplexing model")
	}
	return nil
}

func (c *MultiplexClient) NewRequest() (ClientRequest, error) {
	clireq := c.proto.NewMessage()
	seqid := atomic.AddUint64(&c.seqid, 1)
	if seqid == math.MaxUint64 {
		c.conn.Close()
		return nil, ErrSeqIDExhausted
	}
	clireq.SetSeqID(seqid)
	return newClientRequst(clireq), nil
}

func (c *MultiplexClient) Call(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, ErrClientClosed
	}
	return c.callFunc(ctx, req)
}

func (c *MultiplexClient) call(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	clireq, ok := req.(*clientRequest)
	if !ok {
		return nil, errors.Errorf("hxdtp: unknown request type: %s", reflect.TypeOf(req))
	}
	withresp := &clientSeqIDWithResponse{
		reqid: clireq.SeqID(),
		donec: make(chan error),
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stopc:
		return nil, ErrClientClosed
	case c.regc <- withresp:
	}
	var err error
	defer func() {
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
		case c.deregc <- withresp.reqid:
		}
	}()
	if err = c.send(ctx, req); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-c.donec:
		err = ErrClientClosed
	case <-time.After(c.conf.ReadTimeout):
		err = errors.New("hxdtp: read timedout")
	case <-withresp.donec:
		err = withresp.err
	}
	return withresp.resp, err
}

func (c *MultiplexClient) send(ctx context.Context, req ClientRequest) error {
	c.pl.Lock()
	_ = c.conn.SetWriteDeadline(deadline(ctx, c.conf.WriteTimeout))
	err := c.proto.WriteMessage(req.(*clientRequest).tomsg())
	c.pl.Unlock()
	return err
}

func (c *MultiplexClient) loop() {
	var (
		err      error
		pendings = map[uint64]*clientSeqIDWithResponse{}
		errc     = make(chan error, 1)
	)
	go c.pull(errc)
	defer func() {
		for _, withresp := range pendings {
			withresp.err = err
			close(withresp.donec)
		}
		close(c.donec)
	}()
	for {
		select {
		case <-c.stopc:
			err = ErrClientClosed
			return
		case err = <-errc:
			return
		case withresp := <-c.regc:
			pendings[withresp.reqid] = withresp
		case seqid := <-c.deregc:
			delete(pendings, seqid)
		case resp := <-c.respc:
			if withresp, ok := pendings[resp.SeqID()]; ok {
				delete(pendings, resp.SeqID())
				withresp.resp = resp
				close(withresp.donec)
			}
		}
	}
}

func (c *MultiplexClient) pull(errc chan<- error) {
	// TODO(damnever): split buffered transport for beter resource utilization.
	proto, err := protocol.Build(c.conf.ProtoVersion, newBufferedTransport(c.conn))
	if err != nil {
		errc <- err
		return
	}
	for {
		if atomic.LoadInt32(&c.closed) == 1 {
			return
		}
		resp, err := proto.ReadMessage()
		if err != nil {
			errc <- err
			return
		}
		select {
		case <-c.donec:
			return
		case c.respc <- clientResponse{resp}:
		}
	}
}

func (c *MultiplexClient) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	close(c.stopc)
	<-c.donec
	return c.conn.Close()
}

func newClientRequst(msg protocol.Message) *clientRequest {
	req := &clientRequest{}
	req.reset(msg)
	return req
}

func deadline(ctx context.Context, duration time.Duration) time.Time {
	max := time.Now().Add(duration)
	if dl, ok := ctx.Deadline(); ok && dl.Before(max) {
		return dl
	}
	return max
}

type clientSeqIDWithResponse struct {
	reqid uint64
	resp  clientResponse
	err   error
	donec chan error
}
