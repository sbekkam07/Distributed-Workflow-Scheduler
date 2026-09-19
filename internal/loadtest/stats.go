// Package loadtest calculates reproducible summaries from durable job times.
package loadtest

import (
	"sort"
	"time"
)

// Sample is one terminal job observation returned by the scheduler API.
type Sample struct {
	Status      string
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// LatencySummary reports p50, p95, and p99 without hiding sample count.
type LatencySummary struct {
	Count int           `json:"count"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
}

// Report is a single, machine-readable load-test result. It describes only
// the supplied run and is deliberately not a persisted benchmark claim.
type Report struct {
	Submitted        int            `json:"submitted"`
	Terminal         int            `json:"terminal"`
	Statuses         map[string]int `json:"statuses"`
	Elapsed          time.Duration  `json:"elapsed"`
	JobsPerSecond    float64        `json:"jobs_per_second"`
	QueueLatency     LatencySummary `json:"queue_latency"`
	ExecutionLatency LatencySummary `json:"execution_latency"`
	EndToEndLatency  LatencySummary `json:"end_to_end_latency"`
}

// Summarize calculates throughput and latency percentiles from terminal jobs.
func Summarize(submitted int, started, finished time.Time, samples []Sample) Report {
	report := Report{Submitted: submitted, Terminal: len(samples), Statuses: make(map[string]int), Elapsed: finished.Sub(started)}
	if report.Elapsed > 0 {
		report.JobsPerSecond = float64(report.Terminal) / report.Elapsed.Seconds()
	}
	var queue, execution, endToEnd []time.Duration
	for _, sample := range samples {
		report.Statuses[sample.Status]++
		if sample.CompletedAt != nil && !sample.CompletedAt.Before(sample.CreatedAt) {
			endToEnd = append(endToEnd, sample.CompletedAt.Sub(sample.CreatedAt))
		}
		if sample.StartedAt != nil && !sample.StartedAt.Before(sample.CreatedAt) {
			queue = append(queue, sample.StartedAt.Sub(sample.CreatedAt))
			if sample.CompletedAt != nil && !sample.CompletedAt.Before(*sample.StartedAt) {
				execution = append(execution, sample.CompletedAt.Sub(*sample.StartedAt))
			}
		}
	}
	report.QueueLatency = summarizeDurations(queue)
	report.ExecutionLatency = summarizeDurations(execution)
	report.EndToEndLatency = summarizeDurations(endToEnd)
	return report
}

func summarizeDurations(values []time.Duration) LatencySummary {
	if len(values) == 0 {
		return LatencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return LatencySummary{
		Count: len(sorted),
		P50:   percentile(sorted, 0.50),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
	}
}

func percentile(sorted []time.Duration, percentile float64) time.Duration {
	index := int(percentile*float64(len(sorted)-1) + 0.5)
	return sorted[index]
}
