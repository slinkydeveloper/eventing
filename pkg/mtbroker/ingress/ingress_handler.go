/*
 * Copyright 2019 The Knative Authors
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

package ingress

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cloudevents/sdk-go/v2/extensions"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing/pkg/health"
	"knative.dev/eventing/pkg/kncloudevents"
	broker "knative.dev/eventing/pkg/mtbroker"
	"knative.dev/eventing/pkg/tracing"
	"knative.dev/eventing/pkg/utils"
)

const (
	// noDuration signals that the dispatch step hasn't started
	noDuration = -1
)

type Handler struct {
	// Receiver receives incoming HTTP requests
	Receiver *kncloudevents.HttpMessageReceiver
	// Sender sends requests to the broker
	Sender *kncloudevents.HttpMessageSender
	// Defaults sets default values to incoming events
	Defaulter client.EventDefaulter
	// Reporter reports stats of status code and dispatch time
	Reporter StatsReporter

	Logger *zap.Logger

	getChannelURL func(string, string, string) url.URL
}

func getChannelURL(name, namespace, domain string) url.URL {
	return url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s-kne-trigger-kn-channel.%s.svc.%s", name, namespace, domain),
		Path:   "/",
	}
}

func (h *Handler) Start(ctx context.Context) error {
	if h.getChannelURL == nil {
		h.getChannelURL = getChannelURL
	}

	return h.Receiver.StartListen(ctx, health.WithLivenessCheck(h))
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// validate request method
	if request.Method != http.MethodPost {
		h.Logger.Warn("unexpected request method", zap.String("method", request.Method))
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// validate request URI
	if request.RequestURI == "/" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	nsBrokerName := strings.Split(request.RequestURI, "/")
	if len(nsBrokerName) != 3 {
		h.Logger.Info("Malformed uri", zap.String("URI", request.RequestURI))
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	message := cehttp.NewMessageFromHttpRequest(request)
	defer message.Finish(nil)
	event, err := binding.ToEvent(request.Context(), message)
	if err != nil {
		h.Logger.Warn("failed to extract event from request", zap.Error(err))
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	brokerNamespace := nsBrokerName[1]
	brokerName := nsBrokerName[2]
	brokerNamespacedName := types.NamespacedName{
		Name:      brokerName,
		Namespace: brokerNamespace,
	}
	ctx := request.Context()
	ctx, span := trace.StartSpan(ctx, tracing.BrokerMessagingDestination(brokerNamespacedName))
	defer span.End()

	if span.IsRecordingEvents() {
		span.AddAttributes(
			tracing.MessagingSystemAttribute,
			tracing.MessagingProtocolHTTP,
			tracing.BrokerMessagingDestinationAttribute(brokerNamespacedName),
			tracing.MessagingMessageIDAttribute(event.ID()),
		)
		span.AddAttributes(client.EventTraceAttributes(event)...)
		extensions.FromSpanContext(span.SpanContext()).AddTracingAttributes(event)
	}

	reporterArgs := &ReportArgs{
		ns:        brokerNamespace,
		broker:    brokerName,
		eventType: event.Type(),
	}

	statusCode, dispatchTime := h.receive(ctx, request.Header, event, brokerNamespace, brokerName, span)
	if dispatchTime > noDuration {
		_ = h.Reporter.ReportEventDispatchTime(reporterArgs, statusCode, dispatchTime)
	}
	_ = h.Reporter.ReportEventCount(reporterArgs, statusCode)

	writer.WriteHeader(statusCode)
}

func (h *Handler) receive(ctx context.Context, headers http.Header, event *cloudevents.Event, brokerNamespace, brokerName string, span *trace.Span) (int, time.Duration) {

	// Setting the extension as a string as the CloudEvents sdk does not support non-string extensions.
	event.SetExtension(broker.EventArrivalTime, cloudevents.Timestamp{Time: time.Now()})
	if h.Defaulter != nil {
		newEvent := h.Defaulter(ctx, *event)
		event = &newEvent
	}

	if ttl, err := broker.GetTTL(event.Context); err != nil || ttl <= 0 {
		h.Logger.Debug("dropping event based on TTL status.", zap.Int32("TTL", ttl), zap.String("event.id", event.ID()), zap.Error(err))
		return http.StatusBadRequest, noDuration
	}

	// TODO: Today these are pre-deterministic, change this watch for
	//  	 channels and look up from the channels Status
	channelURI := h.getChannelURL(brokerName, brokerNamespace, utils.GetClusterDomainName())

	return h.send(ctx, headers, event, channelURI.String(), span)
}

func (h *Handler) send(ctx context.Context, headers http.Header, event *cloudevents.Event, target string, span *trace.Span) (int, time.Duration) {

	request, err := h.Sender.NewCloudEventRequestWithTarget(ctx, target)
	if err != nil {
		return http.StatusInternalServerError, noDuration
	}

	message := binding.ToMessage(event)
	defer message.Finish(nil)

	additionalHeaders := utils.PassThroughHeaders(headers)
	err = kncloudevents.WriteHttpRequestWithAdditionalHeaders(ctx, message, request, additionalHeaders)
	if err != nil {
		return http.StatusInternalServerError, noDuration
	}

	resp, dispatchTime, err := h.sendAndRecordDispatchTime(request)
	if err != nil {
		return http.StatusInternalServerError, dispatchTime
	}

	return resp.StatusCode, dispatchTime
}

func (h *Handler) sendAndRecordDispatchTime(request *http.Request) (*http.Response, time.Duration, error) {
	start := time.Now()
	resp, err := h.Sender.Send(request)
	dispatchTime := time.Since(start)
	return resp, dispatchTime, err
}
