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
	"crypto/tls"
	"fmt"
	nethttp "net/http"
	"time"

	"go.opencensus.io/plugin/ochttp"
	"golang.org/x/sync/errgroup"
	"knative.dev/pkg/tracing/propagation/tracecontextb3"

	"knative.dev/eventing/pkg/logging"
)

const (
	DefaultShutdownTimeout = time.Minute * 1
)

type HttpMessageReceiver struct {
	httpPort  int
	httpsPort int

	handler     nethttp.Handler
	httpServer  *nethttp.Server
	httpsServer *nethttp.Server
}

func NewHttpMessageReceiver(httpPort int, httpsPort int) *HttpMessageReceiver {
	return &HttpMessageReceiver{
		httpPort:  httpPort,
		httpsPort: httpsPort,
	}
}

// Blocking
func (recv *HttpMessageReceiver) StartListen(ctx context.Context, handler nethttp.Handler, certs ...tls.Certificate) error {
	recv.handler = CreateHandler(handler)

	recv.httpServer = &nethttp.Server{
		Addr:    fmt.Sprintf(":%d", recv.httpPort),
		Handler: recv.handler,
	}

	errGroup := &errgroup.Group{}
	errGroup.Go(func() error {
		logging.FromContext(ctx).Info("Starting HTTP server")
		return recv.httpServer.ListenAndServe()
	})

	useTlS := len(certs) != 0
	if useTlS {
		recv.httpsServer = &nethttp.Server{
			Addr:    fmt.Sprintf(":%d", recv.httpsPort),
			Handler: recv.handler,
			TLSConfig: &tls.Config{
				Certificates: certs,
			},
		}
		errGroup.Go(func() error {
			logging.FromContext(ctx).Info("Starting HTTPS server")
			return recv.httpsServer.ListenAndServeTLS("", "")
		})
	}

	// wait for the server to return or ctx.Done().
	<-ctx.Done()

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), getShutdownTimeout(ctx))
	defer cancel()

	var errs []error
	if err := recv.httpServer.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}
	if useTlS {
		if err := recv.httpsServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errGroup.Wait(); err != nil {
		errs = append(errs, err)
	}
	if errs != nil && len(errs) != 0 {
		return fmt.Errorf("something broke while waiting for all servers to shutdown: %v", errs)
	}
	return nil
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
		Propagation: tracecontextb3.TraceContextEgress,
		Handler:     handler,
	}
}
