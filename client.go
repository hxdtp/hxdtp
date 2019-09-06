package hxdtp

import (
	"context"
	"fmt"
	"math"
	"net"
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
	ErrSeqIDExhausted = fmt.Errorf("hxdtp: sequence id exhausted")
)

type ClientConfig struct {
	ProtoVersion protocol.Version
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Middlewares  []CallMiddleware
}

func (conf *ClientConfig) withDefaults() {
	if conf.DialTimeout <= 0 {
		conf.DialTimeout = defaultTimeout
	}
	if conf.ReadTimeout <= 0 {
		conf.ReadTimeout = defaultTimeout
	}
	if conf.WriteTimeout <= 0 {
		conf.WriteTimeout = defaultTimeout
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
		return nil, fmt.Errorf("hxdtp: protocol version not registered")
	}
	(&conf).withDefaults()
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
	return c, nil
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
		return nil, ErrSeqIDExhausted
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
		panic("seqid mismatch")
	}
	return clientResponse{cliresp}, nil
}

func (c *Client) Close() error {
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
