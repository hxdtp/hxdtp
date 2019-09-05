package hxdtp

import (
	"bufio"
	"io"
	"net"
	"os"

	"github.com/pkg/errors"
)

const (
	defaultBufferSize = 8 << 10
)

type FileSender interface {
	File() *os.File
	Offset() int64
	Count() int64
	Close() error
}

type fileSender struct {
	FileSender
}

// A dummy method is required.
func (f fileSender) Read([]byte) (int, error) { panic("hxdtp: unreachable - dummy method") }

type bufferedTransport struct {
	bufrw *bufio.ReadWriter
	conn  net.Conn
	buf   [defaultBufferSize]byte
}

func newBufferedTransport(conn net.Conn) *bufferedTransport {
	return &bufferedTransport{
		bufrw: bufio.NewReadWriter(
			bufio.NewReaderSize(conn, defaultBufferSize),
			bufio.NewWriterSize(conn, defaultBufferSize),
		),
		conn: conn,
	}
}

func (t *bufferedTransport) Read(p []byte) (int, error) {
	return t.bufrw.Read(p)
}

func (t *bufferedTransport) Write(p []byte) (int, error) {
	return t.bufrw.Write(p)
}

func (t *bufferedTransport) ReadFrom(r io.Reader) (n int64, err error) {
	if fsr, ok := r.(fileSender); ok {
		// Flush pending data before sendfile call.
		if err = t.bufrw.Flush(); err != nil {
			return
		}
		f := fsr.File()
		if _, err = f.Seek(fsr.Offset(), io.SeekStart); err == nil {
			_, err = io.Copy(t.conn, io.LimitReader(f, fsr.Count()))
		}
	} else {
		_, err = io.CopyBuffer(t.bufrw, r, t.buf[:])
	}
	if err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (t *bufferedTransport) Flush() error {
	return t.bufrw.Flush()
}
