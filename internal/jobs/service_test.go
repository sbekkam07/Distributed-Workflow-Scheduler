package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type memoryRepository struct {
	job       Job
	createErr error
	getErr    error
}

func (r *memoryRepository) Create(_ context.Context, job Job) (Job, error) {
	if r.createErr != nil {
		return Job{}, r.createErr
	}
	r.job = job
	r.job.ID = "d1ec071d-67f7-4aad-ae55-054c1ef3785e"
	return r.job, nil
}

func (r *memoryRepository) CreateOrGet(ctx context.Context, job Job) (Job, bool, error) {
	if r.job.ID != "" && r.job.IdempotencyKey != nil && job.IdempotencyKey != nil && *r.job.IdempotencyKey == *job.IdempotencyKey {
		if r.job.Kind != job.Kind || string(r.job.Payload) != string(job.Payload) || r.job.MaxAttempts != job.MaxAttempts || r.job.Priority != job.Priority {
			return Job{}, false, ErrIdempotencyConflict
		}
		return r.job, false, nil
	}
	created, err := r.Create(ctx, job)
	return created, true, err
}

func (r *memoryRepository) Get(_ context.Context, _ string) (Job, error) {
	if r.getErr != nil {
		return Job{}, r.getErr
	}
	return r.job, nil
}

func TestServiceSubmitIdempotently(t *testing.T) {
	repository := &memoryRepository{}
	service := NewService(repository, time.Now)
	first, err := service.SubmitIdempotently(context.Background(), "echo", json.RawMessage(`{"message":"once"}`), 3, PriorityNormal, time.Now(), "request-key")
	if err != nil || !first.Created {
		t.Fatalf("first SubmitIdempotently() = %+v, %v", first, err)
	}
	second, err := service.SubmitIdempotently(context.Background(), "echo", json.RawMessage(`{"message":"once"}`), 3, PriorityNormal, time.Now(), "request-key")
	if err != nil || second.Created || second.Job.ID != first.Job.ID {
		t.Fatalf("second SubmitIdempotently() = %+v, %v", second, err)
	}
}

func TestServiceSubmitPersistsQueuedJob(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	repository := &memoryRepository{}
	service := NewService(repository, func() time.Time { return now })

	job, err := service.Submit(context.Background(), "echo", json.RawMessage(`{"message":"hello"}`))
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if job.ID == "" || job.Status != StatusQueued {
		t.Errorf("Submit() job = %+v, want persisted queued job", job)
	}
	if !repository.job.CreatedAt.Equal(now) {
		t.Errorf("persisted CreatedAt = %s, want %s", repository.job.CreatedAt, now)
	}
}

func TestServiceGetRejectsInvalidIDBeforeRepository(t *testing.T) {
	service := NewService(&memoryRepository{}, time.Now)

	if _, err := service.Get(context.Background(), "not-a-uuid"); err != ErrInvalidID {
		t.Errorf("Get() error = %v, want %v", err, ErrInvalidID)
	}
}
