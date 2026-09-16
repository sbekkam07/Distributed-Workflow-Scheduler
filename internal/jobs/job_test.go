package jobs

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewCreatesQueuedJob(t *testing.T) {
	now := time.Date(2026, time.September, 13, 14, 30, 0, 0, time.FixedZone("MDT", -6*60*60))

	job, err := New("echo", json.RawMessage(`{"message":"hello"}`), now)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if job.Status != StatusQueued {
		t.Errorf("Status = %s, want %s", job.Status, StatusQueued)
	}
	if !job.CreatedAt.Equal(now.UTC()) {
		t.Errorf("CreatedAt = %s, want %s", job.CreatedAt, now.UTC())
	}
	if job.StartedAt != nil || job.CompletedAt != nil || job.ErrorMessage != nil || job.NextAttemptAt == nil {
		t.Error("new job should not have execution fields")
	}
	if job.AttemptCount != 0 || job.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("new job attempts = %d/%d, want 0/%d", job.AttemptCount, job.MaxAttempts, DefaultMaxAttempts)
	}
}

func TestNewWithMaxAttempts(t *testing.T) {
	now := time.Date(2026, time.September, 13, 14, 30, 0, 0, time.UTC)
	job, err := NewWithMaxAttempts("echo", nil, 5, now)
	if err != nil || job.MaxAttempts != 5 {
		t.Fatalf("NewWithMaxAttempts() = %+v, %v", job, err)
	}
	if _, err := NewWithMaxAttempts("echo", nil, 0, now); err != ErrInvalidMaxAttempts {
		t.Errorf("NewWithMaxAttempts() error = %v, want %v", err, ErrInvalidMaxAttempts)
	}
}

func TestNewRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    string
		payload json.RawMessage
	}{
		{name: "missing kind", payload: json.RawMessage(`{}`)},
		{name: "invalid JSON", kind: "echo", payload: json.RawMessage(`{`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.kind, test.payload, time.Now()); err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}

func TestJobLifecycle(t *testing.T) {
	now := time.Date(2026, time.September, 13, 20, 0, 0, 0, time.UTC)
	job, err := New("echo", nil, now)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := job.Succeed(now); err == nil {
		t.Fatal("Succeed() from QUEUED error = nil, want error")
	}
	if err := job.Start(now.Add(time.Minute)); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if job.AttemptCount != 1 || job.NextAttemptAt != nil {
		t.Errorf("started job retry fields = %d/%v, want 1/nil", job.AttemptCount, job.NextAttemptAt)
	}
	if err := job.Fail("executor returned non-zero", now.Add(2*time.Minute)); err != nil {
		t.Fatalf("Fail() error = %v", err)
	}

	if job.Status != StatusFailed || job.ErrorMessage == nil {
		t.Errorf("job after failure = %+v, want FAILED with error", job)
	}
	if err := job.Start(now.Add(3 * time.Minute)); err == nil {
		t.Fatal("Start() from FAILED error = nil, want error")
	}
}
