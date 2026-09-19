package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a process-local Prometheus registry. Prometheus aggregates the
// same metric names across API, worker, and scheduler scrape targets.
type Metrics struct {
	registry          *prometheus.Registry
	submitted         prometheus.Counter
	completions       *prometheus.CounterVec
	failures          prometheus.Counter
	queueLatency      prometheus.Histogram
	executionDuration prometheus.Histogram
	queueDepth        prometheus.Gauge
	activeWorkers     prometheus.Gauge
}

// New creates an isolated registry so tests and independently running
// processes do not share mutable global metrics.
func New() *Metrics {
	metrics := &Metrics{
		registry: prometheus.NewRegistry(),
		submitted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "scheduler",
			Name:      "jobs_submitted_total",
			Help:      "Total number of newly created jobs.",
		}),
		completions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "scheduler",
			Name:      "job_completions_total",
			Help:      "Total durable job execution outcomes.",
		}, []string{"outcome"}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "scheduler",
			Name:      "job_failures_total",
			Help:      "Total durable failed, dead-lettered, or retried job outcomes.",
		}),
		queueLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "scheduler",
			Name:      "job_queue_latency_seconds",
			Help:      "Time from job creation until a worker claim, including intentional scheduling delay.",
			Buckets:   prometheus.DefBuckets,
		}),
		executionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "scheduler",
			Name:      "job_execution_duration_seconds",
			Help:      "Time spent executing a claimed job before its durable outcome is recorded.",
			Buckets:   prometheus.DefBuckets,
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "scheduler",
			Name:      "queue_depth",
			Help:      "Current count of jobs in the QUEUED state, observed by the scheduler leader.",
		}),
		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "scheduler",
			Name:      "active_workers",
			Help:      "Whether this worker process is active; sum across worker scrape targets.",
		}),
	}
	metrics.registry.MustRegister(
		metrics.submitted,
		metrics.completions,
		metrics.failures,
		metrics.queueLatency,
		metrics.executionDuration,
		metrics.queueDepth,
		metrics.activeWorkers,
	)
	return metrics
}

// Handler exposes this process's Prometheus metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// JobSubmitted records one newly persisted job. Idempotent replays do not call
// this method, so a client retry does not inflate the accepted-job rate.
func (m *Metrics) JobSubmitted() {
	m.submitted.Inc()
}

// JobClaimed records time spent in the durable queue before a worker claim.
func (m *Metrics) JobClaimed(createdAt, claimedAt time.Time) {
	if claimedAt.Before(createdAt) {
		return
	}
	m.queueLatency.Observe(claimedAt.Sub(createdAt).Seconds())
}

// JobFinished records a durable execution outcome and execution duration.
func (m *Metrics) JobFinished(outcome string, duration time.Duration) {
	if duration >= 0 {
		m.executionDuration.Observe(duration.Seconds())
	}
	m.completions.WithLabelValues(outcome).Inc()
	if outcome == "failed" || outcome == "dead" || outcome == "retried" {
		m.failures.Inc()
	}
}

// QueueDepth records the scheduler leader's current queued-job count.
func (m *Metrics) QueueDepth(depth int64) {
	m.queueDepth.Set(float64(depth))
}

// ActiveWorker records whether this worker process is accepting work.
func (m *Metrics) ActiveWorker(active bool) {
	if active {
		m.activeWorkers.Set(1)
		return
	}
	m.activeWorkers.Set(0)
}
