package hxdtp

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hxdtp/hxdtp/protocol"
	protov1 "github.com/hxdtp/hxdtp/protocol/v1"
)

func TestMultiplexing(t *testing.T) {
	protocol.Register(protov1.Version(), protov1.NewBuilder(protov1.WithKeyTable(map[string]uint8{
		"method": 0,
		"sleep":  1,
	})))
	defer protocol.Deregister(protov1.Version())
	addr, err := listenAddr()
	require.Nil(t, err)
	svr, err := NewServer(addr, ServerConfig{
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    time.Second,
		GracefulTimeout: time.Second,
		HandleFunc: func(ctx ServerContext) error {
			ctx.Response().Set("method", ctx.Request().Get("method"))
			time.Sleep(time.Duration(ctx.Request().Get("sleep").(int64)) * time.Millisecond)

			buf := &bytes.Buffer{}
			if _, err0 := io.Copy(buf, ctx.Request().Body()); err != nil {
				return err0
			}
			ctx.Response().WithBlob(buf.Bytes())
			return nil
		},
	})
	require.Nil(t, err)
	go svr.Serve()
	defer svr.Close()

	cli, err := NewMultiplexClient(addr, ClientConfig{
		ProtoVersion: protov1.Version(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	require.Nil(t, err)
	defer cli.Close()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			x := strconv.Itoa(i)
			xx := strings.Repeat(x, 10)
			req, err := cli.NewRequest()
			require.Nil(t, err)
			resp, err := cli.Call(context.TODO(), req.Set(
				"method", x,
			).Set(
				"sleep", i,
			).WithBlob([]byte(xx)))
			require.Nil(t, err)
			require.Equal(t, x, resp.Get("method").(string))
			body, err := ioutil.ReadAll(resp.Body())
			require.Nil(t, err)
			require.Equal(t, xx, string(body))
		}(i)
	}
	wg.Wait()
}

func listenAddr() (addr string, err error) {
	for i := 0; i < 10; i++ {
		var l net.Listener
		l, err = net.Listen("tcp", "localhost:0")
		if err == nil {
			addr = l.Addr().String()
			l.Close()
			return
		}
	}
	return
}
