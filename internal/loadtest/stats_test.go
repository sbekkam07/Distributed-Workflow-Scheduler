package loadtest

import (
	"testing"
	"time"
)

func TestSummarizeUsesDurableTimestamps(t *testing.T) {
	base := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	started := base.Add(time.Second)
	completed := started.Add(2 * time.Second)
	report := Summarize(2, base, base.Add(4*time.Second), []Sample{
		{Status: "SUCCEEDED", CreatedAt: base, StartedAt: &started, CompletedAt: &completed},
		{Status: "BLOCKED", CreatedAt: base, CompletedAt: &completed},
	})
	if report.Terminal != 2 || report.Statuses["SUCCEEDED"] != 1 || report.Statuses["BLOCKED"] != 1 {
		t.Errorf("statuses = %+v, want one succeeded and blocked", report)
	}
	if report.JobsPerSecond != 0.5 {
		t.Errorf("JobsPerSecond = %v, want 0.5", report.JobsPerSecond)
	}
	if report.QueueLatency != (LatencySummary{Count: 1, P50: time.Second, P95: time.Second, P99: time.Second}) {
		t.Errorf("queue latency = %+v", report.QueueLatency)
	}
	if report.ExecutionLatency != (LatencySummary{Count: 1, P50: 2 * time.Second, P95: 2 * time.Second, P99: 2 * time.Second}) {
		t.Errorf("execution latency = %+v", report.ExecutionLatency)
	}
}
