package hxdtp

import "context"

type (
	CallFunc func(context.Context, ClientRequest) (ClientResponse, error)
	// HandleFunc is function to handle server side requests.
	//
	// NOTE: ServerContext is valid for use only until HandleFunc returns, so
	// make a copy of values for long running jobs.
	HandleFunc func(ServerContext) error

	CallMiddleware   func(CallFunc) CallFunc
	HandleMiddleware func(HandleFunc) HandleFunc
)
