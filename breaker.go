// Copyright 2023 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cse

import (
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker"
)

const (
	open uint32 = iota
	closed
)

var (
	errUnavailable = errors.New("resource unavailable")
)

type asyncBreaker struct {
	cb    *gobreaker.CircuitBreaker
	state uint32
	done  chan struct{}
	once  sync.Once

	probeMinInterval time.Duration
	probeMaxInterval time.Duration
}

type settings struct {
	Name             string
	MaxRequests      uint32
	Interval         time.Duration
	Timeout          time.Duration
	ProbeMinInterval time.Duration
	ProbeMaxInterval time.Duration
	ReadyToTrip      func(counts gobreaker.Counts) bool
	IsSuccessful     func(err error) bool
	Probe            func(string) error
}

func newAsyncBreaker(s settings) *asyncBreaker {
	if s.ProbeMinInterval < 0 || s.ProbeMaxInterval < 0 || s.ProbeMinInterval > s.ProbeMaxInterval {
		panic("invalid probe interval settings")
	}
	breaker := &asyncBreaker{
		state: closed,
		done:  make(chan struct{}, 1),

		probeMinInterval: s.ProbeMinInterval,
		probeMaxInterval: s.ProbeMaxInterval,
	}
	cbs := gobreaker.Settings{
		Name:         s.Name,
		MaxRequests:  s.MaxRequests,
		Interval:     s.Interval,
		Timeout:      s.Timeout,
		ReadyToTrip:  s.ReadyToTrip,
		IsSuccessful: s.IsSuccessful,
	}
	cbs.OnStateChange = func(_ string, from gobreaker.State, to gobreaker.State) {
		if from == gobreaker.StateClosed && to == gobreaker.StateOpen {
			breaker.openWith(s.Probe)
		}
	}
	breaker.cb = gobreaker.NewCircuitBreaker(cbs)
	return breaker
}

func (b *asyncBreaker) Close() {
	b.once.Do(func() {
		b.done <- struct{}{}
		close(b.done)
	})
}

func (b *asyncBreaker) openWith(probe func(string) error) bool {
	success := atomic.CompareAndSwapUint32(&b.state, closed, open)
	if success {
		go b.probeLoop(probe)
	}
	return success
}

func (b *asyncBreaker) nextProbeInterval() time.Duration {
	return b.probeMinInterval + time.Duration(rand.Int64N(int64(b.probeMaxInterval)-int64(b.probeMinInterval)))
}

func (b *asyncBreaker) probeLoop(probe func(string) error) {
	ticker := time.NewTicker(b.nextProbeInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := probe(b.cb.Name())
			if err != nil {
				ticker.Reset(b.nextProbeInterval())
				continue
			}
			atomic.CompareAndSwapUint32(&b.state, open, closed)
			return
		case <-b.done:
			return
		}
	}
}

func (b *asyncBreaker) Execute(f func() (any, error)) (any, error) {
	return b.cb.Execute(func() (any, error) {
		if atomic.LoadUint32(&b.state) == open {
			return nil, errUnavailable
		}
		return f()
	})
}
