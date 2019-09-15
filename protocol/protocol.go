package protocol

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
)

type (
	Headers interface {
		Get(key string) interface{}
		Set(key string, value interface{})
		Del(key string)
	}
	Message interface {
		Reset()
		SeqID() uint64
		SetSeqID(uint64)
		// SyncFrom(Message)
		Headers() Headers
		// NOTE(damnever): I use io.LimitedReader rather than *io.LimitedReader here,
		// that is not a mistake, since what I actually need is a Reader with the length.
		Body() io.LimitedReader
		SetBody(io.LimitedReader)
	}
	WithMessageOption func(Message)
	VersionedProtocol interface {
		Version() Version
		ReadMessage(Message) error
		WriteMessage(Message) error
	}
	Flusher interface {
		Flush() error
	}
	WithProtocolOption func(VersionedProtocol)
	Builder            struct {
		NewMessage           func() (Message, error)
		NewVersionedProtocol func(Version, io.ReadWriter) (VersionedProtocol, error)
	}
)

type Version struct {
	Major uint8
	Minor uint8
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

func (v Version) bytes() []byte {
	return []byte{v.Major, v.Minor}
}

var (
	identify       = [...]byte{'H', 'X', 'D', 'T', 'P', '-'}
	defaultFactory = NewFactory()
)

type Factory struct {
	l        sync.RWMutex
	builders map[uint8]Builder
}

func NewFactory() *Factory {
	return &Factory{
		builders: map[uint8]Builder{},
	}
}

func (f *Factory) Registered(version Version) bool {
	f.l.RLock()
	_, ok := f.builders[version.Major]
	f.l.RUnlock()
	return ok
}

func (f *Factory) Register(version Version, builder Builder) error {
	f.l.Lock()
	defer f.l.Unlock()
	major := version.Major
	if _, ok := f.builders[major]; ok {
		return errors.Errorf("hxdtp/protocol: major version '%x' has been registered", major)
	}
	f.builders[major] = builder
	return nil
}

func (f *Factory) Deregister(version Version) {
	f.l.Lock()
	delete(f.builders, version.Major)
	f.l.Unlock()
}

func (f *Factory) NewMessage(version Version) (Message, error) {
	f.l.RLock()
	builder, ok := f.builders[version.Major]
	f.l.RUnlock()
	if !ok {
		return nil, errors.Errorf("hxdtp/protocol: unknown/unregistered protocol version '%+v'", version)
	}
	return builder.NewMessage()
}

func (f *Factory) NewProtocol(version Version, transport io.ReadWriter) (VersionedProtocol, error) {
	f.l.RLock()
	builder, ok := f.builders[version.Major]
	f.l.RUnlock()
	if !ok {
		return nil, errors.Errorf("hxdtp/protocol: unknown/unregistered protocol version '%+v'", version)
	}
	return builder.NewVersionedProtocol(version, transport)
}

func (f *Factory) SelectProtocol(transport io.ReadWriter) (VersionedProtocol, error) {
	var p [8]byte
	_, err := io.ReadFull(transport, p[:])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	protoname := p[:6]
	version := Version{Major: p[6], Minor: p[7]}

	if !bytes.Equal(protoname, identify[:]) {
		return nil, errors.Errorf("hxdtp/protocol: unknown protocol name '%s'", protoname)
	}
	return f.NewProtocol(version, transport)
}

func (f *Factory) NewProtocolWithIdentify(version Version, transport io.ReadWriter) (VersionedProtocol, error) {
	if _, err := transport.Write(identify[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := transport.Write(version.bytes()); err != nil {
		return nil, errors.WithStack(err)
	}
	if f, ok := transport.(Flusher); ok {
		if err := f.Flush(); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return f.NewProtocol(version, transport)
}

func Registered(version Version) bool {
	return defaultFactory.Registered(version)
}

func Register(version Version, builder Builder) error {
	return defaultFactory.Register(version, builder)
}

func Deregister(version Version) {
	defaultFactory.Deregister(version)
}

func NewMessage(version Version) (Message, error) {
	return defaultFactory.NewMessage(version)
}

func NewProtocol(version Version, transport io.ReadWriter) (VersionedProtocol, error) {
	return defaultFactory.NewProtocol(version, transport)
}

func SelectProtocol(transport io.ReadWriter) (VersionedProtocol, error) {
	return defaultFactory.SelectProtocol(transport)
}

func NewProtocolWithIdentify(version Version, transport io.ReadWriter) (VersionedProtocol, error) {
	return defaultFactory.NewProtocolWithIdentify(version, transport)
}
