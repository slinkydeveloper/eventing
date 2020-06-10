/*
 * Copyright 2020 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kncloudevents

import (
	"context"
	"fmt"
	"net"
	nethttp "net/http"
	"time"

	nethttp2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
)

const (
	DefaultShutdownTimeout = time.Minute * 1
)

type HttpMessageReceiver struct {
	port int

	handler  nethttp.Handler
	h1s      *nethttp.Server
	h2s      *nethttp2.Server
	listener net.Listener
}

func NewHttpMessageReceiver(port int) *HttpMessageReceiver {
	return &HttpMessageReceiver{
		port: port,
	}
}

// Blocking
func (recv *HttpMessageReceiver) StartListen(ctx context.Context, handler nethttp.Handler) error {
	var err error
	if recv.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", recv.port)); err != nil {
		return err
	}

	recv.handler = CreateHandler(handler)

	recv.h2s = &nethttp2.Server{}
	recv.h1s = &nethttp.Server{
		Addr: recv.listener.Addr().String(),
		// h2c handler adds support for HTTP/2 Cleartext
		Handler: h2c.NewHandler(recv.handler, recv.h2s),
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- recv.h1s.Serve(recv.listener)
	}()

	// wait for the server to return or ctx.Done().
	select {
	case <-ctx.Done():
		ctx, cancel := context.WithTimeout(context.Background(), getShutdownTimeout(ctx))
		defer cancel()
		err := recv.h1s.Shutdown(ctx)
		<-errChan // Wait for server goroutine to exit
		return err
	case err := <-errChan:
		return err
	}
}

type shutdownTimeoutKey struct{}

func getShutdownTimeout(ctx context.Context) time.Duration {
	v := ctx.Value(shutdownTimeoutKey{})
	if v == nil {
		return DefaultShutdownTimeout
	}
	return v.(time.Duration)
}

func WithShutdownTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, shutdownTimeoutKey{}, timeout)
}

func CreateHandler(handler nethttp.Handler) nethttp.Handler {
	return &ochttp.Handler{
		Propagation: &tracecontext.HTTPFormat{},
		Handler:     handler,
	}
}
