package v1

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"

	"github.com/hxdtp/xdcodec"
)

type headers struct {
	keytbl  KeyTable
	static  TableDrivenMap
	dynamic xdcodec.Map
}

func newHeaders(keytbl KeyTable) headers {
	return headers{
		keytbl:  keytbl,
		static:  TableDrivenMap{},
		dynamic: xdcodec.Map{},
	}
}

func (h headers) Reset() {
	h.static.Reset()
	h.dynamic.Reset()
}

func (h headers) Get(key string) interface{} {
	var val xdcodec.Typed
	if h.keytbl.HasKey(key) {
		val = h.static[key]
	} else {
		val = h.dynamic[key]
	}

	var value interface{}
	switch x := val.(type) {
	case xdcodec.TypedInt:
		value = int64(x)
	case xdcodec.TypedUint:
		value = uint64(x)
	case xdcodec.TypedFloat:
		value = float64(x)
	case xdcodec.TypedBytes:
		value = []byte(x)
	case xdcodec.TypedString:
		value = string(x)
	}
	return value
}

func (h headers) Set(key string, value interface{}) {
	var val xdcodec.Typed
	// TODO: support all types.
	switch x := value.(type) {
	case int:
		val = xdcodec.TypedInt(x)
	case int8:
		val = xdcodec.TypedInt(x)
	case int16:
		val = xdcodec.TypedInt(x)
	case int32:
		val = xdcodec.TypedInt(x)
	case int64:
		val = xdcodec.TypedInt(x)
	case uint:
		val = xdcodec.TypedUint(x)
	case uint8:
		val = xdcodec.TypedUint(x)
	case uint16:
		val = xdcodec.TypedUint(x)
	case uint32:
		val = xdcodec.TypedUint(x)
	case uint64:
		val = xdcodec.TypedUint(x)
	case float32:
		val = xdcodec.TypedFloat(x)
	case float64:
		val = xdcodec.TypedFloat(x)
	case []byte:
		val = xdcodec.TypedBytes(x)
	case string:
		val = xdcodec.TypedString(x)
	default:
		panic(fmt.Sprintf("unsupported type: %v", reflect.TypeOf(x)))
	}

	if h.keytbl.HasKey(key) {
		h.static[key] = val
	} else {
		h.dynamic[key] = val
	}
}

func (h headers) Del(key string) {
	if h.keytbl.HasKey(key) {
		delete(h.static, key)
	} else {
		delete(h.dynamic, key)
	}
}

func (h headers) readFrom(codec *xdcodec.Codec) error {
	if h.static == nil {
		h.static = TableDrivenMap{}
	}
	if err := h.static.ReadWith(codec, h.keytbl); err != nil {
		return err
	}
	if err := codec.ReadMap(&h.dynamic); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (h headers) writeTo(codec *xdcodec.Codec) error {
	if err := h.static.WriteWith(codec, h.keytbl); err != nil {
		return err
	}
	if err := codec.WriteMap(h.dynamic); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
