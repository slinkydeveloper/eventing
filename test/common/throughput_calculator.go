/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"time"
)

type result bool

const (
	count result = true
	end   result = false
)

type ThroughputCalculator struct {
	OnNewMeasurement func(time time.Time, thpt uint32)
	results          chan result
}

func NewThroughputCalculator(onNewMeasurement func(time time.Time, thpt uint32), channelSize uint32) ThroughputCalculator {
	return ThroughputCalculator{
		OnNewMeasurement: onNewMeasurement,
		results:          make(chan result, channelSize),
	}
}

func (calculator ThroughputCalculator) Count() {
	calculator.results <- count
}

func (calculator ThroughputCalculator) End() {
	calculator.results <- end
}

func (calculator ThroughputCalculator) RunAndWaitEnd() {
	ticker := time.NewTicker(1 * time.Second)
	var count uint32

	for {
		select {
		case r, ok := <-calculator.results:
			if ok {
				if r == end {
					return
				} else {
					count++
				}
			}
		case t := <-ticker.C:
			calculator.OnNewMeasurement(t, count)
			count = 0
		}
	}
}
