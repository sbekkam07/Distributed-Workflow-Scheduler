package jobs

import (
	"context"
	"encoding/json"
	"time"
)

// Repository persists and retrieves jobs. Implementations must use the durable
// store as the source of truth for generated IDs and timestamps.
type Repository interface {
	Create(context.Context, Job) (Job, error)
	CreateOrGet(context.Context, Job) (Job, bool, error)
	Get(context.Context, string) (Job, error)
}

// Submission is a durable job and whether this request created it. A repeated
// idempotent submission returns the original job with Created set to false.
type Submission struct {
	Job     Job
	Created bool
}

// Service contains job-submission and lookup use cases shared by transports.
type Service struct {
	repository Repository
	now        func() time.Time
}

// NewService constructs a job service. The clock is injected to make job
// creation deterministic in tests.
func NewService(repository Repository, now func() time.Time) *Service {
	return &Service{repository: repository, now: now}
}

// Submit validates a new job and persists it in the queued state.
func (s *Service) Submit(ctx context.Context, kind string, payload json.RawMessage) (Job, error) {
	return s.SubmitWithMaxAttempts(ctx, kind, payload, DefaultMaxAttempts)
}

// SubmitWithMaxAttempts validates and persists a job with its total execution
// budget. The first execution counts as attempt one.
func (s *Service) SubmitWithMaxAttempts(ctx context.Context, kind string, payload json.RawMessage, maxAttempts int) (Job, error) {
	return s.SubmitWithOptions(ctx, kind, payload, maxAttempts, DefaultPriority)
}

// SubmitWithOptions validates and persists a job with its execution budget and
// priority.
func (s *Service) SubmitWithOptions(ctx context.Context, kind string, payload json.RawMessage, maxAttempts int, priority Priority) (Job, error) {
	job, err := NewWithOptions(kind, payload, maxAttempts, priority, s.now())
	if err != nil {
		return Job{}, err
	}
	return s.repository.Create(ctx, job)
}

// SubmitIdempotently creates a job once for key. Repeating the same request
// returns the original job; reusing key for different work is rejected.
func (s *Service) SubmitIdempotently(ctx context.Context, kind string, payload json.RawMessage, maxAttempts int, priority Priority, key string) (Submission, error) {
	job, err := NewWithOptions(kind, payload, maxAttempts, priority, s.now())
	if err != nil {
		return Submission{}, err
	}
	job, err = job.WithIdempotencyKey(key)
	if err != nil {
		return Submission{}, err
	}
	persisted, created, err := s.repository.CreateOrGet(ctx, job)
	if err != nil {
		return Submission{}, err
	}
	return Submission{Job: persisted, Created: created}, nil
}

// Get returns the durable representation of a job.
func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	if err := ValidateID(id); err != nil {
		return Job{}, err
	}
	return s.repository.Get(ctx, id)
}
