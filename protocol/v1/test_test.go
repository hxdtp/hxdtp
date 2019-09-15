package v1

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO(damnever): more test cases

func TestProtocol(t *testing.T) {
	buf := &bytes.Buffer{}
	table := map[string]uint8{"Method": 1}

	p := newProto(buf)
	m := newMessage(WithKeyTable(table))
	m.SetSeqID(123)
	m.Headers().Set("Method", "DEL")
	m.Headers().Set("TAKE", "AWAY")
	m.Headers().Set("INT", 1)
	body := "HAHAHA"
	bodyR := bytes.NewBufferString(body)
	m.SetBody(io.LimitedReader{R: bodyR, N: int64(bodyR.Len())})
	require.Nil(t, p.WriteMessage(m))

	p2 := newProto(bytes.NewBuffer(buf.Bytes()))
	m2 := newMessage(WithKeyTable(table))
	err := p2.ReadMessage(m2)
	require.Nil(t, err)

	m.SetBody(io.LimitedReader{})
	m2body, err := ioutil.ReadAll(m2.Body().R)
	require.Nil(t, err)
	m2.SetBody(io.LimitedReader{})

	require.Equal(t, m, m2)
	require.Equal(t, body, string(m2body))
}
