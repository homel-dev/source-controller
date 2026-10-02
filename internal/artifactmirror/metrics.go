/*
Copyright 2026 The Flux authors

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

package artifactmirror

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	MirrorAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "artifact_mirror_attempts_total",
			Help: "Total number of artifact mirror attempts",
		},
		[]string{"result"},
	)
	MirrorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "artifact_mirror_duration_seconds",
			Help: "Duration of artifact mirror operations",
		},
		[]string{"result"},
	)
	MirrorBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "artifact_mirror_uploaded_bytes_total",
			Help: "Total bytes uploaded to the artifact mirror",
		},
	)
	MirrorLastSuccess = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "artifact_mirror_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful artifact mirror reconciliation",
		},
		[]string{"cluster", "namespace", "name"},
	)
)

func init() {
	metrics.Registry.MustRegister(MirrorAttempts, MirrorDuration, MirrorBytes, MirrorLastSuccess)
}
