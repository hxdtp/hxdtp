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
	ClientRequest  ServerResponse
	ClientResponse ServerRequest
	clientRequest  = serverResponse
	clientResponse = serverRequest

	ClientOptions struct {
		RemoteAddr     string
		ProtoVersion   protocol.Version
		ConnectTimeout time.Duration
		ReadTimeout    time.Duration
		WriteTimeout   time.Duration
	}
)

var (
	ErrSeqIDExhausted = fmt.Errorf("sequence id exhausted")
)

type Client struct {
	opts  ClientOptions
	seqid uint64
	conn  net.Conn
	proto protocol.VersionedProtocol
}

func NewClient(opts ClientOptions) (*Client, error) {
	dialer := net.Dialer{Timeout: opts.ConnectTimeout}
	conn, err := dialer.Dial("tcp", opts.RemoteAddr)
	if err != nil {
		return nil, err
	}
	proto, err := protocol.Connect(opts.ProtoVersion, newBufferedTransport(conn))
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Client{
		opts:  opts,
		seqid: 0,
		conn:  conn,
		proto: proto,
	}, nil
}

// TODO(damnever): NewClientWithConn

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
	clireq, ok := req.(*clientRequest)
	if !ok {
		return nil, ErrSeqIDExhausted
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := c.proto.WriteMessage(clireq.tomsg()); err != nil {
		return nil, err
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
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
