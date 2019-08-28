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
	"github.com/cloudevents/sdk-go/pkg/cloudevents/transport/http"
	vegeta "github.com/tsenart/vegeta/lib"
	"knative.dev/eventing/test/common"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	"knative.dev/eventing/test/performance"
	pkgpacers "knative.dev/pkg/test/vegeta/pacers"
)

const (
	defaultEventType   = "perf-test-event-type"
	defaultEventSource = "perf-test-event-source"

	// The interval and timeout used for polling in checking event states.
	pollInterval = 1 * time.Second
	pollTimeout  = 4 * time.Minute
)

// flags for the image
var (
	sinkURL            string
	msgSize            int
	eventNum           int
	errorRateThreshold float64
	encoding           string
	eventTimeMap       map[string]chan time.Time
	sentCh             chan sentState
	receivedCh         chan receivedState
	resultCh           chan result
	secondDuration     int
	rps                int
)

// eventStatus is status of the event, for now only if all events are in received status can the
// test be considered as PASS.
type eventStatus int

const (
	sent eventStatus = iota
	received
	undelivered
	dropped
	duplicated
	corrupted // TODO(Fredy-Z): corrupted status is not being used now
)

// state saves the data that is used to generate the metrics
//type state struct {
//	latency time.Duration
//	status  eventStatus
//}

type result struct {
	sendLatency       time.Duration
	e2eReceiveLatency time.Duration
	status            eventStatus
}

type state struct {
	eventId uint64
	at      time.Time
}

type receivedState state
type sentState state

func init() {
	flag.StringVar(&sinkURL, "sink", "", "The sink URL for the event destination.")
	flag.IntVar(&msgSize, "msg-size", 100, "The size of each message we want to send. Generate random strings to avoid caching.")
	flag.IntVar(&eventNum, "event-count", 10, "The number of events we want to send.")
	flag.IntVar(&secondDuration, "duration", 20, "Duration of the benchmark in seconds")
	flag.Float64Var(&errorRateThreshold, "error-rate-threshold", 0.1, "Rate of error event deliveries we allow. We fail the test if the error rate crosses the threshold.")
	flag.StringVar(&encoding, "encoding", "binary", "The encoding of the cloud event, one of(binary, structured).")
	flag.IntVar(&rps, "rps", 1000, "Maximum request per seconds")
}

func main() {
	// parse the command line flags
	flag.Parse()

	// We don't know how messages are sent, so we estimate is at most the rate at maximum pace * duration of the benchmark
	pessimisticNumberOfTotalMessages := rps * secondDuration

	// We estimate that the channel reader requires at most 3 seconds to process a message
	pessimisticNumberOfMessagesInsideAChannel := rps * 3

	sentCh = make(chan sentState, pessimisticNumberOfMessagesInsideAChannel)
	receivedCh = make(chan receivedState, pessimisticNumberOfMessagesInsideAChannel)
	resultCh = make(chan result, pessimisticNumberOfTotalMessages)

	println("Starting cloud event processor")

	startCloudEventsReceiver()
	go processE2ELatency()

	targeter := common.NewCloudEventsTargeter(sinkURL, msgSize, defaultEventType, defaultEventSource, encoding).VegetaTargeter()

	pacer, err := pkgpacers.NewSteadyUp(
		vegeta.Rate{
			Freq: 100,
			Per:  time.Second,
		},
		vegeta.Rate{
			Freq: rps,
			Per:  time.Second,
		},
		2*time.Second,
	)

	if err != nil {
		failTest(fmt.Sprintf("failed to create pacer: %v\n", err))
	}

	// sleep 30 seconds before sending the events
	// TODO(Fredy-Z): this is a bit hacky, as ideally, we need to wait for the Trigger/Subscription that uses it as a
	//                Subscriber to become ready before sending the events, but we don't have a way to coordinate between them.
	//time.Sleep(30 * time.Second)

	println("Starting attack")

	vegetaResults := vegeta.NewAttacker(vegeta.MaxWorkers(1), vegeta.Workers(1)).Attack(targeter, pacer, time.Duration(secondDuration)*time.Second, defaultEventType+"-attack")

	println("Starting attack processor")

	go processVegetaResult(vegetaResults)

	// export result for this test
	exportTestResult()
}

func processVegetaResult(vegetaResults <-chan *vegeta.Result) {
	i := 0
	for res := range vegetaResults {
		fmt.Printf("Vegeta result %d\n", i)
		sentCh <- sentState{eventId: res.Seq, at: res.Timestamp} // res.Timestamp is the time when the request was fired!
		resultCh <- result{sendLatency: res.Latency, status: sent}
	}
	close(sentCh)

	// Let's assume that after 5 seconds all responses are received and processed
	time.Sleep(5 * time.Second)
	fmt.Printf("Closing receivedCh")
	close(receivedCh)

	time.Sleep(time.Second)
	fmt.Printf("Closing resultCh")
	close(resultCh)
}

func startCloudEventsReceiver() {
	// get encoding
	var encodingOption http.Option
	switch encoding {
	case "binary":
		encodingOption = cloudevents.WithBinaryEncoding()
	case "structured":
		encodingOption = cloudevents.WithStructuredEncoding()
	default:
		failTest(fmt.Sprintf("unsupported encoding option: %q\n", encoding))
	}

	t, err := cloudevents.NewHTTPTransport(
		cloudevents.WithTarget(sinkURL),
		encodingOption,
	)
	if err != nil {
		failTest(fmt.Sprintf("failed to create transport: %v\n", err))
	}
	c, err := cloudevents.NewClient(t,
		cloudevents.WithTimeNow(),
		cloudevents.WithUUIDs(),
	)
	if err != nil {
		failTest(fmt.Sprintf("failed to create client: %v\n", err))
	}

	go c.StartReceiver(context.Background(), processReceiveEvent)
}

func processReceiveEvent(event cloudevents.Event) {
	id, _ := strconv.ParseUint(event.ID(), 10, 64)
	receivedCh <- receivedState{id, time.Now()}
}

func processE2ELatency() {
	sentEventsMap := make(map[uint64]time.Time)
	for {
		select {
		case s, ok := <-sentCh:
			if ok {
				sentEventsMap[s.eventId] = s.at
				fmt.Printf("Sent %d at %s", s.eventId, s.at.String())
			} else {
				fmt.Printf("SentCh closed")
			}
		case r, ok := <-receivedCh:
			if ok {
				timestampSent, ok := sentEventsMap[r.eventId]
				if ok {
					e2eLatency := r.at.Sub(timestampSent)
					fmt.Printf("Received %d at %s", r.eventId, r.at.String())
					resultCh <- result{e2eReceiveLatency: e2eLatency, status: received}
				} else {
					fmt.Printf("Corrupted")
					resultCh <- result{status: corrupted}
				}
			} else {
				fmt.Printf("ReceivedCh closed")
				return
			}
		}
	}
}

func exportTestResult() {
	// number of abnormal event deliveries
	var errorCount int
	var sendLatency = make([]int64, 0)
	var e2eLatency = make([]int64, 0)
	for eventState := range resultCh {
		println("New result")
		switch eventState.status {
		case sent:
			sendLatency = append(sendLatency, int64(eventState.sendLatency))
		case received:
			e2eLatency = append(e2eLatency, int64(eventState.e2eReceiveLatency))
		case dropped, duplicated, undelivered:
			errorCount++
		default:
			errorCount++
		}
	}

	// if the error rate is larger than the threshold, we consider this test to be failed
	if errorCount != 0 && float64(errorCount)/float64(eventNum) > errorRateThreshold {
		failTest(fmt.Sprintf("%d events failed to deliver", errorCount))
	}

	// use the stringbuilder to build the test result
	var builder strings.Builder
	builder.WriteString("\n")
	builder.WriteString(performance.TestResultKey + ": " + performance.TestPass)
	builder.WriteString("\n")

	// create latency metrics
	for _, perc := range []float64{0.50, 0.90, 0.99} {
		samplePercentile := float32(calculateSamplePercentile(sendLatency, perc)) / float32(1e9)
		name := fmt.Sprintf("p%d(s)", int(perc*100))
		builder.WriteString(fmt.Sprintf("%s: %f\n", name, samplePercentile))
	}
	for _, perc := range []float64{0.50, 0.90, 0.99} {
		samplePercentile := float32(calculateSamplePercentile(e2eLatency, perc)) / float32(1e9)
		name := fmt.Sprintf("p%d(s)", int(perc*100))
		builder.WriteString(fmt.Sprintf("%s: %f\n", name, samplePercentile))
	}

	log.Printf(builder.String())
}

func failTest(reason string) {
	var builder strings.Builder
	builder.WriteString("\n")
	builder.WriteString(performance.TestResultKey + ": " + performance.TestFail + "\n")
	builder.WriteString(performance.TestFailReason + ": " + reason)
	log.Fatalf(builder.String())
	os.Exit(1)
}
