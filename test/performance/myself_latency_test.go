// +build performance

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

package performance

import (
	"log"
	"sort"
	"strconv"
	"testing"

	"knative.dev/eventing/test/base/resources"
	"knative.dev/eventing/test/common"
	"knative.dev/test-infra/shared/junit"
	"knative.dev/test-infra/shared/testgrid"
)

func TestMyselfLatency(t *testing.T) {
	testMyselfLatency(t)
}

func testMyselfLatency(t *testing.T) {
	const (
		senderName     = "perf-latency-sender"
		latencyPodName = "perf-latency-pod"
	)

	client := common.Setup(t, false)
	//defer common.TearDown(client)

	// create event latency measurement service
	latencyPod := resources.EventLatencyPod(latencyPodName, "http://localhost:8080", 1000)
	client.CreatePodOrFail(latencyPod, common.WithService(latencyPodName))

	// wait for all test resources to be ready, so that we can start sending events
	if err := client.WaitForAllTestResourcesReady(); err != nil {
		t.Fatalf("Failed to get all test resources ready: %v", err)
	}

	// parse test result from the Pod log
	res, err := ParseTestResultFromLog(client.Kube, latencyPodName, latencyPod.Spec.Containers[0].Name, client.Namespace)
	if err != nil {
		t.Fatalf("failed to get the test result: %v", err)
	}

	// fail the test directly if the result is TestFail
	if testResult, ok := res[TestResultKey]; ok {
		if testResult == TestFail {
			t.Fatalf("error happens when running test in the pod: %s", res[TestFailReason])
		}
	}

	// collect the metricNames and sort them
	metricNames := make([]string, 0, len(res))
	for key := range res {
		if key == TestResultKey {
			continue
		}
		metricNames = append(metricNames, key)
	}
	sort.Strings(metricNames)

	// create latency metrics and save them as XML files that can be parsed by Testgrid
	var tc []junit.TestCase
	for _, metricName := range metricNames {
		metricValue := res[metricName]
		floatMetricValue, err := strconv.ParseFloat(metricValue, 64)
		if err != nil {
			t.Fatalf("unknown metric value %s for %s", metricValue, metricName)
		}
		tc = append(tc, CreatePerfTestCase(float32(floatMetricValue), metricName, t.Name()))
	}

	if err := testgrid.CreateXMLOutput(tc, t.Name()); err != nil {
		log.Fatalf("Cannot create output xml: %v", err)
	}
}
