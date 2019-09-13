package hxdtp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/hxdtp/hxdtp/protocol"
	protov1 "github.com/hxdtp/hxdtp/protocol/v1"
)

func Example() {
	// NOTE:
	// For forwards- or backwards-compatibility, DO NOT changes the
	// mapping relation in KeyTable if you do not known what you are
	// doing, you can mark the useless one as reserved, in other words,
	// name it as __RESERVED_XX__ until you can safely reuse it.
	protocol.Register(protov1.Version(), protov1.Builder(protov1.WithKeyTable(map[string]uint8{
		"Method": 0,
	})))
	defer protocol.Deregister(protov1.Version())
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	addr, err := listenAddr() // Random listen address.
	must(err)
	svr, err := NewServer(addr, ServerConfig{
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    time.Second,
		GracefulTimeout: time.Second,
		HandleFunc: func(ctx ServerContext) error {
			ctx.Response().Set("Method", ctx.Request().Get("Method"))
			buf := &bytes.Buffer{}
			buf.WriteString(ctx.Request().Get("X-Whatever").(string))
			buf.WriteString(" - ")
			if _, err0 := io.Copy(buf, ctx.Request().Body()); err != nil {
				return err0
			}
			buf.WriteString(" - TODO")
			ctx.Response().WithBlob(buf.Bytes())
			return nil
		},
	})
	must(err)
	go svr.Serve()
	defer svr.Close()

	time.Sleep(100 * time.Millisecond)

	cli, err := NewClient(addr, ClientConfig{
		ProtoVersion: protov1.Version(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	must(err)
	defer cli.Close()
	req, err := cli.NewRequest()
	must(err)
	resp, err := cli.Call(context.TODO(), req.Set(
		"Method", "GO",
	).Set(
		"X-Whatever", "HXDTP",
	).WithBlob([]byte("loading awesomeness")))
	must(err)
	fmt.Println(resp.Get("Method"))
	body, err := ioutil.ReadAll(resp.Body())
	must(err)
	fmt.Println(string(body))

	// Output:
	// GO
	// HXDTP - loading awesomeness - TODO
}
