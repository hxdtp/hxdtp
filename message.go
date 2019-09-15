package hxdtp

import (
	"bytes"
	"io"

	"github.com/hxdtp/hxdtp/protocol"
)

type (
	readonlyMessager interface {
		// TODO: method to get all kvs by prefix
		Get(key string) interface{}
		Body() io.Reader
	}
	writeableMessager interface {
		Set(key string, value interface{}) writeableMessager
		WithBlob(p []byte) writeableMessager
		WithStream(rd io.LimitedReader) writeableMessager
		WithFile(fsr FileSender) writeableMessager
	}
)

type readonlyMessage struct {
	protocol.Message
}

func newReadonlyMessage(raw protocol.Message) *readonlyMessage {
	return &readonlyMessage{Message: raw}
}

func (r *readonlyMessage) reset() {
	r.Message.Reset()
}

func (r *readonlyMessage) Get(key string) interface{} {
	return r.Message.Headers().Get(key)
}

func (r *readonlyMessage) Body() io.Reader {
	return r.Message.Body().R
}

type writeableMessage struct {
	protocol.Message
	rdc *readerChain
}

func newWriteableMessage(raw protocol.Message) *writeableMessage {
	return &writeableMessage{
		Message: raw,
		rdc:     &readerChain{},
	}
}

func (m *writeableMessage) reset() {
	m.Message.Reset()
	m.rdc.Reset()
}

func (m *writeableMessage) tomsg() protocol.Message {
	m.Message.SetBody(io.LimitedReader{
		R: m.rdc,
		N: m.rdc.size,
	})
	return m.Message
}

func (m *writeableMessage) Set(key string, value interface{}) writeableMessager {
	m.Message.Headers().Set(key, value)
	return m
}

func (m *writeableMessage) WithBlob(p []byte) writeableMessager {
	m.rdc.Append(bytes.NewBuffer(p), int64(len(p)))
	return m
}

func (m *writeableMessage) WithStream(rd io.LimitedReader) writeableMessager {
	m.rdc.Append(rd.R, rd.N)
	return m
}

func (m *writeableMessage) WithFile(fsr FileSender) writeableMessager {
	m.rdc.Append(fileSender{fsr}, fsr.Count())
	return m
}

type readerChain struct {
	chain []io.Reader
	size  int64
}

func (rc *readerChain) Reset() {
	if rc.chain != nil {
		for i := range rc.chain {
			rc.chain[i] = nil
		}
		rc.chain = rc.chain[:0]
	}
	rc.size = 0
}

func (rc *readerChain) Append(r io.Reader, size int64) {
	rc.chain = append(rc.chain, r)
	rc.size += size
}

func (rc *readerChain) Size() int64 {
	return rc.size
}

func (rc *readerChain) Read(p []byte) (int, error) {
	panic("hxdtp: unreachable - also not implemented")
}

// WriteTo implements io.WriterTo, it is use io.Copy internally,
// so caller can implements io.ReaderFrom to avoid allocation.
func (rc *readerChain) WriteTo(w io.Writer) (n int64, err error) {
	for _, r := range rc.chain {
		nw, err0 := io.Copy(w, r)
		n += nw
		rc.size -= nw
		if err0 != nil {
			err = err0
			return
		}
	}
	return
}
