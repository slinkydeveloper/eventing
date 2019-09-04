/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"knative.dev/eventing/test/common"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	vegeta "github.com/tsenart/vegeta/lib"
	"knative.dev/pkg/signals"
	"knative.dev/pkg/test/mako"
	pkgpacers "knative.dev/pkg/test/vegeta/pacers"
)

const (
	defaultEventType             = "continue.benchmark"
	defaultBenchmarkEndEventType = "end.benchmark"
	defaultEventSource           = "perf-test-event-source"
	defaultDuration              = 10 * time.Second
)

// flags for the image
var (
	sinkURL  string
	msgSize  int
	workers  uint64
	paceFlag string
	verbose  bool
	fatalf   = log.Fatalf
)

type requestInterceptor struct {
	before func(*http.Request)
	after  func(*http.Request, *http.Response, error)
}

func (r requestInterceptor) RoundTrip(request *http.Request) (*http.Response, error) {
	if r.before != nil {
		r.before(request)
	}
	res, err := http.DefaultTransport.RoundTrip(request)
	if r.after != nil {
		r.after(request, res, err)
	}
	return res, err
}

func init() {
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	flag.StringVar(&sinkURL, "sink", "", "The sink URL for the event destination.")
	flag.IntVar(&msgSize, "msg-size", 100, "The size in bytes of each message we want to send. Generate random strings to avoid caching.")
	flag.StringVar(&paceFlag, "pace", "", "Pace array comma separated. Format rps[:duration=10s]. Example 100,200:4,100:1,500:60")
	flag.Uint64Var(&workers, "workers", 1, "Number of vegeta workers")
}

type paceSpec struct {
	rps      int
	duration time.Duration
}

func main() {
	// parse the command line flags
	flag.Parse()

	if paceFlag == "" {
		fatalf("pace not set!")
	}

	if sinkURL == "" {
		fatalf("sink not set!")
	}

	pacerSpecs, err := parsePaceSpec()
	if err != nil {
		fatalf("%+v", err)
	}

	// --- Configure mako

	// We want this for properly handling Kubernetes container lifecycle events.
	ctx := signals.NewContext()

	// Use the benchmark key created
	ctx, q, qclose, err := mako.Setup(ctx)
	if err != nil {
		log.Fatalf("Failed to setup mako: %v", err)
	}

	// Use a fresh context here so that our RPC to terminate the sidecar
	// isn't subject to our timeout (or we won't shut it down when we time out)
	defer qclose(context.Background())

	// Wrap fatalf in a helper or our sidecar will live forever.
	fatalf = func(f string, args ...interface{}) {
		qclose(context.Background())
		log.Fatalf(f, args...)
	}

	printf("Mako configured")

	// --- Warmup phase

	printf("--- BENCHMARK ---")
	printf("Configuring channels")

	// --- Allocate channels

	// We need those estimates to allocate memory before benchmark starts
	var estimatedNumberOfMessagesInsideAChannel uint32

	for _, pacer := range pacerSpecs {
		// Queueing theory stuff: Given our channels can process 50 rps, queueLength = arrival rps / 50 rps = pacer.rps / 50
		queueLength := uint32(pacer.rps / 50)
		if queueLength < 10 {
			queueLength = 10
		}
		if queueLength > estimatedNumberOfMessagesInsideAChannel {
			estimatedNumberOfMessagesInsideAChannel = queueLength
		}
	}

	printf("Estimated channel size: %v", estimatedNumberOfMessagesInsideAChannel)

	throughputCalculator := common.NewThroughputCalculator(func(time time.Time, thpt uint32) {
		if qerr := q.AddSamplePoint(mako.XTime(time), map[string]float64{"dt": float64(thpt)}); qerr != nil {
			log.Printf("ERROR AddSamplePoint: %v", qerr)
		}
	}, estimatedNumberOfMessagesInsideAChannel)

	// sleep 30 seconds before sending the events
	// TODO(Fredy-Z): this is a bit hacky, as ideally, we need to wait for the Trigger/Subscription that uses it as a
	//                Subscriber to become ready before sending the events, but we don't have a way to coordinate between them.
	time.Sleep(30 * time.Second)

	targeter := common.NewCloudEventsTargeter(sinkURL, msgSize, defaultEventType, defaultEventSource, "binary").VegetaTargeter()

	pacers := make([]vegeta.Pacer, len(pacerSpecs))
	durations := make([]time.Duration, len(pacerSpecs))
	var totalBenchmarkDuration time.Duration = 0

	for i, ps := range pacerSpecs {
		pacers[i] = vegeta.ConstantPacer{ps.rps, time.Second}
		durations[i] = ps.duration
		printf("%d° pace: %d rps for %v seconds", i+1, ps.rps, ps.duration)
		totalBenchmarkDuration = totalBenchmarkDuration + ps.duration
	}

	printf("Total benchmark duration: %v", totalBenchmarkDuration.Seconds())

	combinedPacer, err := pkgpacers.NewCombined(pacers, durations)

	if err != nil {
		fatalf("Failed to setup combined pacer: %v", err)
	}

	client := http.Client{Transport: requestInterceptor{after: func(request *http.Request, response *http.Response, e error) {
		if !(e != nil || response.StatusCode < 200 || response.StatusCode > 300) {
			throughputCalculator.Count()
		}
	}}}

	printf("Starting benchmark")

	vegetaResults := vegeta.NewAttacker(
		vegeta.Client(&client),
		vegeta.Workers(workers),
		vegeta.MaxWorkers(workers),
	).Attack(targeter, combinedPacer, totalBenchmarkDuration, defaultEventType+"-attack")

	go func() {
		for _ = range vegetaResults {
		}

		sendStopEvent()
		throughputCalculator.End()
	}()

	throughputCalculator.RunAndWaitEnd()

	printf("--- END BENCHMARK ---")

	out, err := q.Store()
	if err != nil {
		fatalf("q.Store error: %v: %v", out, err)
	}
}

func parsePaceSpec() ([]paceSpec, error) {
	paceSpecArray := strings.Split(paceFlag, ",")
	pacerSpecs := make([]paceSpec, 0)

	for _, p := range paceSpecArray {
		ps := strings.Split(p, ":")
		rps, err := strconv.Atoi(ps[0])
		if err != nil {
			return nil, fmt.Errorf("error while parsing pace spec %v: %v", ps, err)
		}
		duration := defaultDuration

		if len(ps) == 2 {
			durationSec, err := strconv.Atoi(ps[1])
			if err != nil {
				return nil, fmt.Errorf("error while parsing pace spec %v: %v", ps, err)
			}
			duration = time.Second * time.Duration(durationSec)
		}

		pacerSpecs = append(pacerSpecs, paceSpec{rps, duration})
	}

	return pacerSpecs, nil
}

func sendStopEvent() {
	t, err := cloudevents.NewHTTPTransport(
		cloudevents.WithTarget(sinkURL),
		cloudevents.WithBinaryEncoding(),
	)
	if err != nil {
		fatalf("failed to create transport: %v\n", err)
	}
	c, err := cloudevents.NewClient(t,
		cloudevents.WithTimeNow(),
		cloudevents.WithUUIDs(),
	)
	if err != nil {
		fatalf("failed to create client: %v\n", err)
	}

	event := cloudevents.NewEvent()
	event.SetID("-1")
	event.SetType(defaultBenchmarkEndEventType)
	event.SetSource(defaultEventSource)

	_, _, _ = c.Send(context.TODO(), event)

}

func printf(f string, args ...interface{}) {
	if verbose {
		log.Printf(f, args...)
	}
}
