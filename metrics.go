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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/metrics"
)

var (
	TiKVSyncRegionDuration *prometheus.HistogramVec
)

func init() {
	InitMetrics("tikv", "client", nil)
}

func InitMetrics(namespace, subsystem string, constLabels map[string]string) {
	TiKVSyncRegionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "sync_region_duration",
			Help:        "Bucketed histogram of the duration of sync region.",
			ConstLabels: constLabels,
			Buckets:     prometheus.ExponentialBuckets(0.0005, 2, 28),
		}, []string{metrics.LblType})
}

func RegisterMetrics() {
	prometheus.MustRegister(TiKVSyncRegionDuration)
}
