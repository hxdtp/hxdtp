package hxdtp

import "context"

type (
	CallFunc   func(context.Context, ClientRequest) (ClientResponse, error)
	HandleFunc func(ServerContext) error

	CallMiddleware   func(CallFunc) CallFunc
	HandleMiddleware func(HandleFunc) HandleFunc
)
