package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	// ErrInvalidKind indicates that a submission did not specify an executor.
	ErrInvalidKind = errors.New("job kind is required")
	// ErrInvalidPayload indicates that the job payload is not valid JSON.
	ErrInvalidPayload = errors.New("job payload must be valid JSON")
	// ErrInvalidID indicates that a request did not provide a canonical UUID.
	ErrInvalidID = errors.New("job ID must be a UUID")
	// ErrNotFound indicates that no persisted job has the requested ID.
	ErrNotFound = errors.New("job not found")
	// ErrNotRunning indicates that a worker attempted a terminal update for a
	// job whose durable state is no longer RUNNING.
	ErrNotRunning = errors.New("job is not running")
	// ErrLeaseLost indicates that a worker no longer owns an unexpired lease.
	ErrLeaseLost = errors.New("job lease is no longer owned by this worker")
	// ErrInvalidMaxAttempts indicates that a job's retry limit is not positive.
	ErrInvalidMaxAttempts = errors.New("max attempts must be a positive integer")
	// ErrInvalidIdempotencyKey indicates an unusable client-supplied key.
	ErrInvalidIdempotencyKey = errors.New("idempotency key must be between 1 and 255 characters")
	// ErrIdempotencyConflict indicates a key was reused for a different job.
	ErrIdempotencyConflict = errors.New("idempotency key was already used for a different job")
	// ErrInvalidPriority indicates an unsupported job priority.
	ErrInvalidPriority = errors.New("priority must be HIGH, NORMAL, or LOW")
	// ErrInvalidDependencies indicates malformed or duplicate prerequisite IDs.
	ErrInvalidDependencies = errors.New("dependencies must contain unique job UUIDs")
	// ErrDependencyNotFound indicates a submitted prerequisite does not exist.
	ErrDependencyNotFound = errors.New("job dependency not found")
)

// DefaultMaxAttempts is the total number of executions allowed for a job,
// including its first execution.
const DefaultMaxAttempts = 3

// Priority controls claim ordering among eligible queued jobs.
type Priority string

const (
	PriorityHigh   Priority = "HIGH"
	PriorityNormal Priority = "NORMAL"
	PriorityLow    Priority = "LOW"
)

const DefaultPriority = PriorityNormal

var canonicalUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Status describes the durable lifecycle of a job.
type Status string

const (
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusDead      Status = "DEAD"
	StatusBlocked   Status = "BLOCKED"
)

// Job is one independently executable unit of work.
//
// Kind selects an executor in a later increment. Payload is executor-specific
// JSON, deliberately kept opaque to the scheduler's core.
type Job struct {
	ID              string          `json:"id"`
	Kind            string          `json:"kind"`
	Payload         json.RawMessage `json:"payload"`
	Status          Status          `json:"status"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	LeaseOwner      *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at,omitempty"`
	AttemptCount    int             `json:"attempt_count"`
	MaxAttempts     int             `json:"max_attempts"`
	NextAttemptAt   *time.Time      `json:"next_attempt_at,omitempty"`
	IdempotencyKey  *string         `json:"idempotency_key,omitempty"`
	Priority        Priority        `json:"priority"`
	RunAt           time.Time       `json:"run_at"`
	DependsOn       []string        `json:"depends_on,omitempty"`
}

// ValidateDependencies accepts only unique, existing-job-shaped UUIDs. Since
// edges are immutable and prerequisites must already exist, this also prevents
// dependency cycles at submission time.
func ValidateDependencies(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if ValidateID(id) != nil {
			return ErrInvalidDependencies
		}
		if _, ok := seen[id]; ok {
			return ErrInvalidDependencies
		}
		seen[id] = struct{}{}
	}
	return nil
}

// New creates a queued job that is ready for a worker to claim.
func New(kind string, payload json.RawMessage, now time.Time) (Job, error) {
	return NewWithMaxAttempts(kind, payload, DefaultMaxAttempts, now)
}

// NewWithMaxAttempts creates a queued job with an explicit total attempt
// budget. An attempt includes the first execution and every retry.
func NewWithMaxAttempts(kind string, payload json.RawMessage, maxAttempts int, now time.Time) (Job, error) {
	return NewWithOptions(kind, payload, maxAttempts, DefaultPriority, now)
}

// NewWithOptions creates a queued job with an execution budget and priority.
func NewWithOptions(kind string, payload json.RawMessage, maxAttempts int, priority Priority, now time.Time) (Job, error) {
	return NewWithSchedule(kind, payload, maxAttempts, priority, now, now)
}

// NewWithSchedule creates a queued job that cannot be claimed before runAt.
// A time in the past is valid and makes work eligible immediately.
func NewWithSchedule(kind string, payload json.RawMessage, maxAttempts int, priority Priority, runAt, now time.Time) (Job, error) {
	if strings.TrimSpace(kind) == "" {
		return Job{}, ErrInvalidKind
	}
	if maxAttempts < 1 {
		return Job{}, ErrInvalidMaxAttempts
	}
	if !priority.Valid() {
		return Job{}, ErrInvalidPriority
	}

	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return Job{}, ErrInvalidPayload
	}

	return Job{
		Kind:          kind,
		Payload:       payload,
		Status:        StatusQueued,
		CreatedAt:     now.UTC(),
		MaxAttempts:   maxAttempts,
		NextAttemptAt: timePointer(now.UTC()),
		Priority:      priority,
		RunAt:         runAt.UTC(),
	}, nil
}

// Valid reports whether p is a supported strict priority.
func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityNormal || p == PriorityLow
}

// WithIdempotencyKey attaches a client-supplied stable key to a new job. The
// API uses it to turn repeated submissions into the same durable job.
func (j Job) WithIdempotencyKey(key string) (Job, error) {
	key = strings.TrimSpace(key)
	if len(key) == 0 || len(key) > 255 {
		return Job{}, ErrInvalidIdempotencyKey
	}
	j.IdempotencyKey = &key
	return j, nil
}

// EffectKey is stable across every lease recovery and retry. Executors that
// cause external effects must pass it to a target that supports idempotency.
// If a client did not supply a key, the durable job ID is the effect key.
func (j Job) EffectKey() string {
	if j.IdempotencyKey != nil {
		return *j.IdempotencyKey
	}
	return j.ID
}

func timePointer(value time.Time) *time.Time {
	return &value
}

// ValidateID verifies that id uses the UUID format generated by PostgreSQL.
func ValidateID(id string) error {
	if !canonicalUUID.MatchString(id) {
		return ErrInvalidID
	}
	return nil
}

// Start records that a worker began executing a queued job.
func (j *Job) Start(now time.Time) error {
	if j.Status != StatusQueued {
		return fmt.Errorf("cannot start job in %s state", j.Status)
	}

	startedAt := now.UTC()
	j.Status = StatusRunning
	j.StartedAt = &startedAt
	j.NextAttemptAt = nil
	j.AttemptCount++
	return nil
}

// Succeed records successful completion of a running job.
func (j *Job) Succeed(now time.Time) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("cannot succeed job in %s state", j.Status)
	}

	completedAt := now.UTC()
	j.Status = StatusSucceeded
	j.CompletedAt = &completedAt
	j.ErrorMessage = nil
	j.NextAttemptAt = nil
	return nil
}

// Fail records an execution failure for a running job.
func (j *Job) Fail(message string, now time.Time) error {
	if j.Status != StatusRunning {
		return fmt.Errorf("cannot fail job in %s state", j.Status)
	}
	if strings.TrimSpace(message) == "" {
		return fmt.Errorf("failure message is required")
	}

	completedAt := now.UTC()
	j.Status = StatusFailed
	j.CompletedAt = &completedAt
	j.ErrorMessage = &message
	j.NextAttemptAt = nil
	return nil
}
