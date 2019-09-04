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
	cloudevents "github.com/cloudevents/sdk-go"
	"knative.dev/eventing/test/common"
	"knative.dev/pkg/signals"
	"knative.dev/pkg/test/mako"
	"log"
	"time"
)

// flags for the image
var (
	verbose         bool
	maxThptExpected int
	fatalf          = log.Fatalf
)

func init() {
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	flag.IntVar(&maxThptExpected, "max-throughput-expected", 0, "Max throughput expected in rps. This is required to preallocate as much memory as possible")
}

func main() {
	// parse the command line flags
	flag.Parse()

	if maxThptExpected <= 0 {
		log.Fatalf("max-throughput-expected must be > 0")
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

	printf("--- BENCHMARK ---")

	printf("Creating result channel")

	// Queueing theory stuff: Given our channels can process 50 rps, queueLength = maxThptExpected / 50 rps = maxThptExpected / 50
	var estimatedNumberOfMessagesInsideAChannel = uint32(maxThptExpected / 50)

	if estimatedNumberOfMessagesInsideAChannel < 10 {
		estimatedNumberOfMessagesInsideAChannel = 10
	}

	// Instantiate the throughput calculator
	var throughputCalculator = common.NewThroughputCalculator(func(time time.Time, thpt uint32) {
		if qerr := q.AddSamplePoint(mako.XTime(time), map[string]float64{"dt": float64(thpt)}); qerr != nil {
			log.Printf("ERROR AddSamplePoint: %v", qerr)
		}
	}, estimatedNumberOfMessagesInsideAChannel)

	printf("Starting CloudEvents receiver")

	t, err := cloudevents.NewHTTPTransport(
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

	go c.StartReceiver(context.Background(), func(event cloudevents.Event) {
		if event.Type() == "end.benchmark" {
			throughputCalculator.End()
		} else {
			throughputCalculator.Count()
		}
	})

	throughputCalculator.RunAndWaitEnd()

	// Publish the store
	out, err := q.Store()
	if err != nil {
		fatalf("q.Store error: %v: %v", out, err)
	}
}

func printf(f string, args ...interface{}) {
	if verbose {
		log.Printf(f, args...)
	}
}
