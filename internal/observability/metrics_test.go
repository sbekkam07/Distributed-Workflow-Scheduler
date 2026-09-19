package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerExposesOperationalMetrics(t *testing.T) {
	metrics := New()
	metrics.JobSubmitted()
	metrics.JobClaimed(time.Unix(0, 0), time.Unix(2, 0))
	metrics.JobFinished("succeeded", time.Second)
	metrics.JobFinished("retried", 2*time.Second)
	metrics.QueueDepth(4)
	metrics.ActiveWorker(true)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, metric := range []string{
		"scheduler_jobs_submitted_total 1",
		`scheduler_job_completions_total{outcome="succeeded"} 1`,
		`scheduler_job_completions_total{outcome="retried"} 1`,
		"scheduler_job_failures_total 1",
		"scheduler_queue_depth 4",
		"scheduler_active_workers 1",
		"scheduler_job_queue_latency_seconds",
		"scheduler_job_execution_duration_seconds",
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics response missing %q:\n%s", metric, body)
		}
	}
}
