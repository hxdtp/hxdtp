package v1

import (
	"errors"
	"fmt"

	"github.com/hxdtp/xdcodec"
)

var (
	ErrNotOneToOne   = errors.New("hxdtp/protocol/v1: not one to one relationship")
	ErrTableTooLarge = errors.New("hxdtp/protocol/v1: table limit 255")
)

type TableDrivenMap map[string]xdcodec.Typed

type KeyTable struct {
	mapping    map[string]uint8
	revmapping map[uint8]string
}

func NewKeyTable(m map[string]uint8) (KeyTable, error) {
	if len(m) > 255 {
		return KeyTable{}, ErrTableTooLarge
	}
	r := map[uint8]string{}
	for k, id := range m {
		r[id] = k
	}
	if len(m) != len(r) {
		return KeyTable{}, ErrNotOneToOne
	}
	return KeyTable{mapping: m, revmapping: r}, nil
}

func (kt KeyTable) HasKey(key string) bool {
	_, ok := kt.mapping[key]
	return ok
}

func (m TableDrivenMap) Reset() {
	for k := range m {
		delete(m, k)
	}
}

func (m TableDrivenMap) ReadWith(codec *xdcodec.Codec, tbl KeyTable) error {
	var nkv uint8
	if err := codec.ReadUint8(&nkv); err != nil {
		return err
	}
	if nkv == 0 {
		return nil
	}

	for i := uint8(0); i < nkv; i++ {
		var id uint8
		if err := codec.ReadUint8(&id); err != nil {
			return err
		}
		k, ok := tbl.revmapping[id]
		if !ok {
			return fmt.Errorf("id '%d' not found in table", id)
		}
		v, err := codec.ReadTyped()
		if err != nil {
			return err
		}
		m[k] = v
	}
	return nil
}

func (m TableDrivenMap) WriteWith(codec *xdcodec.Codec, tbl KeyTable) error {
	n := len(m)
	if n > xdcodec.ContainerCapacity {
		return xdcodec.ErrExceedContainerCap
	}

	if err := codec.WriteUint8(uint8(n)); err != nil {
		return err
	}
	for k, v := range m {
		id, ok := tbl.mapping[k]
		if !ok {
			return fmt.Errorf("key '%s' not found in table", k)
		}
		if err := codec.WriteUint8(id); err != nil {
			return err
		}
		if err := codec.WriteTyped(v); err != nil {
			return err
		}
	}
	return nil
}
