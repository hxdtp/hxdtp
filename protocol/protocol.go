package protocol

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
)

type (
	Version struct {
		Major uint8
		Minor uint8
	}
	Headers interface {
		Get(key string) interface{}
		Set(key string, value interface{})
		Del(key string)
	}
	Message interface {
		SeqID() uint64
		SetSeqID(uint64)
		Headers() Headers
		// NOTE(damnever): I use io.LimitedReader rather than *io.LimitedReader here,
		// that is not a mistake, since what I actually need is a Reader with the length.
		Body() io.LimitedReader
		SetBody(io.LimitedReader)
	}
	VersionedProtocol interface {
		Version() Version
		ReadMessage() (Message, error)
		WriteMessage(Message) error
		NewMessage() Message
		NewMessageFrom(req Message) Message
	}
	Flusher interface {
		Flush() error
	}
	WithOption               func(VersionedProtocol)
	VersionedProtocolBuilder func(Version, io.ReadWriter) VersionedProtocol
)

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

func (v Version) bytes() []byte {
	return []byte{v.Major, v.Minor}
}

var (
	identify = [...]byte{'H', 'X', 'D', 'T', 'P', '-'}
	registry = &struct {
		sync.RWMutex
		protocols map[uint8]VersionedProtocolBuilder
	}{
		protocols: map[uint8]VersionedProtocolBuilder{},
	}
)

func Registered(version Version) bool {
	registry.RLock()
	_, ok := registry.protocols[version.Major]
	registry.RUnlock()
	return ok
}

func Register(version Version, builder VersionedProtocolBuilder) {
	registry.Lock()
	defer registry.Unlock()
	major := version.Major
	if _, ok := registry.protocols[major]; ok {
		panic(fmt.Sprintf("Major version '%x' has been registered", major))
	}
	registry.protocols[major] = builder
}

func Build(version Version, transport io.ReadWriter) (VersionedProtocol, error) {
	registry.RLock()
	builder, ok := registry.protocols[version.Major]
	registry.RUnlock()
	if !ok {
		return nil, errors.Errorf("Unknown/unregistered protocol version '%+v'", version)
	}
	return builder(version, transport), nil
}

func Select(transport io.ReadWriter) (VersionedProtocol, error) {
	var p [8]byte
	_, err := io.ReadFull(transport, p[:])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	protoname := p[:6]
	version := Version{Major: p[6], Minor: p[7]}

	if !bytes.Equal(protoname, identify[:]) {
		return nil, errors.Errorf("Unknown protocol name '%s'", protoname)
	}
	return Build(version, transport)
}

func Connect(version Version, transport io.ReadWriter) (VersionedProtocol, error) {
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
	return Build(version, transport)
}
