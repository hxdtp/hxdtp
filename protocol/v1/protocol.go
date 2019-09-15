package v1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/hxdtp/hxdtp/protocol"
	"github.com/hxdtp/xdcodec"
)

var (
	ErrUnknownRequestType = fmt.Errorf("unknown request type for current version")

	version = protocol.Version{Major: 1, Minor: 0}
)

func Version() protocol.Version {
	return version
}

func NewBuilder(msgopts ...protocol.WithMessageOption) protocol.Builder {
	// FIXME
	return protocol.Builder{
		NewMessage: func() (protocol.Message, error) {
			return newMessage(msgopts...), nil
		},
		NewVersionedProtocol: func(_ protocol.Version, transport io.ReadWriter) (protocol.VersionedProtocol, error) {
			return newProto(transport), nil
		},
	}
}

func WithKeyTable(tbl map[string]uint8) protocol.WithMessageOption {
	keytable, err := NewKeyTable(tbl)
	if err != nil {
		panic(err)
	}
	return func(m protocol.Message) {
		m.(*message).keytable = keytable
	}
}

type proto struct {
	transport io.ReadWriter
	codec     *xdcodec.Codec
	buf       *bytes.Buffer
	bbuf      [8192]byte
}

func newProto(transport io.ReadWriter, opts ...protocol.WithProtocolOption) protocol.VersionedProtocol {
	p := &proto{
		transport: transport,
		codec:     xdcodec.New(binary.BigEndian, nil),
		buf:       &bytes.Buffer{},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *proto) Version() protocol.Version {
	return version
}

func (p *proto) ReadMessage(m protocol.Message) error {
	rawm, ok := m.(*message)
	if !ok {
		return ErrUnknownRequestType
	}
	if rawm == nil {
		return errors.Errorf("hxdtp/protocol/v1: unwritable message value")
	}
	err := p.readMsg(rawm)
	return err
}

func (p *proto) WriteMessage(m protocol.Message) error {
	rawm, ok := m.(*message)
	if !ok {
		return ErrUnknownRequestType
	}
	return p.writeMsg(*rawm)
}

func (p *proto) readMsg(m *message) error {
	codec := p.codec
	codec.Reset(p.transport)
	var (
		size uint32
		uv   uint64
	)
	if err := codec.ReadUint32(&size); err != nil {
		return errors.WithStack(err)
	}
	// FIXME(damnever): too ugly
	reader := io.LimitReader(p.transport, int64(size))
	codec.Reset(xdcodec.WithReadWriter{Reader: reader, Writer: xdcodec.NopWriter})

	if err := codec.ReadUvarint(&uv); err != nil {
		return errors.WithStack(err)
	}
	m.seqid = uv
	if err := m.headers.readFrom(codec); err != nil {
		return errors.WithStack(err)
	}
	buf := &bytes.Buffer{}
	if _, err := io.CopyBuffer(buf, reader, p.bbuf[:]); err != nil {
		return errors.WithStack(err)
	}
	m.body = io.LimitedReader{R: buf, N: int64(buf.Len())}
	return nil
}

func (p *proto) writeMsg(m message) error {
	codec := p.codec
	buf := p.buf
	buf.Reset()
	codec.Reset(buf)

	if err := codec.WriteUint32(0); err != nil {
		return errors.WithStack(err)
	}
	if err := codec.WriteUvarint(m.seqid); err != nil {
		return errors.WithStack(err)
	}
	if err := m.headers.writeTo(codec); err != nil {
		return errors.WithStack(err)
	}
	header := buf.Bytes()
	codec.Reset(bytes.NewBuffer(header[:0]))
	size := buf.Len() - 4 + int(m.body.N)
	if err := codec.WriteUint32(uint32(size)); err != nil {
		return errors.WithStack(err)
	}

	if _, err := p.transport.Write(header); err != nil {
		return errors.WithStack(err)
	}
	if m.body.N > 0 {
		if _, err := io.CopyBuffer(p.transport, m.body.R, p.bbuf[:]); err != nil {
			return errors.WithStack(err)
		}
	}
	if f, ok := p.transport.(protocol.Flusher); ok {
		if err := f.Flush(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type message struct {
	keytable KeyTable
	headers  headers
	body     io.LimitedReader

	seqid uint64
}

func newMessage(opts ...protocol.WithMessageOption) *message {
	m := &message{}
	for _, opts := range opts {
		opts(m)
	}
	m.headers = newHeaders(m.keytable)
	return m
}

func (m *message) Reset() {
	m.body.R = nil
	m.body.N = 0
	m.seqid = 0
	m.headers.Reset()
}

func (m *message) SeqID() uint64 {
	return m.seqid
}

func (m *message) SetSeqID(seqid uint64) {
	m.seqid = seqid
}

func (m *message) Headers() protocol.Headers {
	return m.headers
}

func (m *message) Body() io.LimitedReader {
	return m.body
}

func (m *message) SetBody(body io.LimitedReader) {
	m.body = body
}
