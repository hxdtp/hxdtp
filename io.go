package hxdtp

import "io"

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
