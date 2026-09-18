package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

const testJobID = "d1ec071d-67f7-4aad-ae55-054c1ef3785e"

type fakeRepository struct {
	jobs      map[string]jobs.Job
	nextID    string
	createErr error
	getErr    error
}

func (r *fakeRepository) Create(_ context.Context, job jobs.Job) (jobs.Job, error) {
	if r.createErr != nil {
		return jobs.Job{}, r.createErr
	}
	job.ID = r.nextID
	r.jobs[job.ID] = job
	return job, nil
}

func (r *fakeRepository) CreateOrGet(ctx context.Context, job jobs.Job) (jobs.Job, bool, error) {
	if job.IdempotencyKey != nil {
		for _, existing := range r.jobs {
			if existing.IdempotencyKey != nil && *existing.IdempotencyKey == *job.IdempotencyKey {
				if existing.Kind != job.Kind || string(existing.Payload) != string(job.Payload) || existing.MaxAttempts != job.MaxAttempts || existing.Priority != job.Priority {
					return jobs.Job{}, false, jobs.ErrIdempotencyConflict
				}
				return existing, false, nil
			}
		}
	}
	created, err := r.Create(ctx, job)
	return created, true, err
}

func (r *fakeRepository) Get(_ context.Context, id string) (jobs.Job, error) {
	if r.getErr != nil {
		return jobs.Job{}, r.getErr
	}
	job, ok := r.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return job, nil
}

func newTestHandler() http.Handler {
	repository := &fakeRepository{jobs: make(map[string]jobs.Job), nextID: testJobID}
	service := jobs.NewService(repository, func() time.Time {
		return time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	})
	return NewHandler(service)
}

func TestCreateJob(t *testing.T) {
	handler := newTestHandler()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","payload":{"message":"hello"}}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusCreated, response.Body.String())
	}
	var job jobs.Job
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.ID != testJobID || job.Status != jobs.StatusQueued {
		t.Errorf("job = %+v, want persisted queued job", job)
	}
}

func TestCreateJobRejectsInvalidRequest(t *testing.T) {
	for _, body := range []string{
		`{"payload":{}}`,
		`{"kind":"echo","unexpected":true}`,
		`{"kind":"echo"} {"kind":"second"}`,
		`not JSON`,
		`{"kind":"echo","max_attempts":0}`,
	} {
		t.Run(body, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
			response := httptest.NewRecorder()

			newTestHandler().ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestCreateJobAcceptsMaxAttempts(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","max_attempts":5}`))
	response := httptest.NewRecorder()

	newTestHandler().ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusCreated, response.Body.String())
	}
	var job jobs.Job
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", job.MaxAttempts)
	}
}

func TestCreateJobAcceptsPriority(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","priority":"HIGH"}`))
	response := httptest.NewRecorder()

	newTestHandler().ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusCreated, response.Body.String())
	}
	var job jobs.Job
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.Priority != jobs.PriorityHigh {
		t.Errorf("Priority = %s, want %s", job.Priority, jobs.PriorityHigh)
	}
}

func TestCreateJobAcceptsRunAt(t *testing.T) {
	runAt := "2026-09-20T12:00:00Z"
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","run_at":"`+runAt+`"}`))
	response := httptest.NewRecorder()

	newTestHandler().ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusCreated, response.Body.String())
	}
	var job jobs.Job
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.RunAt.Format(time.RFC3339) != runAt {
		t.Errorf("RunAt = %s, want %s", job.RunAt, runAt)
	}
}

func TestCreateJobIdempotencyKeyReturnsOriginalJob(t *testing.T) {
	handler := newTestHandler()
	body := `{"kind":"echo","payload":{"message":"hello"}}`

	first := httptest.NewRecorder()
	firstRequest := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	firstRequest.Header.Set("Idempotency-Key", "client-request-7")
	handler.ServeHTTP(first, firstRequest)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201; body = %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	secondRequest := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	secondRequest.Header.Set("Idempotency-Key", "client-request-7")
	handler.ServeHTTP(second, secondRequest)
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, want 200; body = %s", second.Code, second.Body.String())
	}

	var firstJob, secondJob jobs.Job
	if err := json.NewDecoder(first.Body).Decode(&firstJob); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if err := json.NewDecoder(second.Body).Decode(&secondJob); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if firstJob.ID != secondJob.ID || secondJob.IdempotencyKey == nil || *secondJob.IdempotencyKey != "client-request-7" {
		t.Errorf("idempotent responses = %+v / %+v, want same keyed job", firstJob, secondJob)
	}
}

func TestCreateJobRejectsIdempotencyKeyConflict(t *testing.T) {
	handler := newTestHandler()
	first := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","payload":{"message":"first"}}`))
	first.Header.Set("Idempotency-Key", "same-key")
	handler.ServeHTTP(httptest.NewRecorder(), first)

	second := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","payload":{"message":"second"}}`))
	second.Header.Set("Idempotency-Key", "same-key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, second)
	if response.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d; body = %s", response.Code, http.StatusConflict, response.Body.String())
	}
}

func TestCreateJobRejectsInvalidPriority(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo","priority":"URGENT"}`))
	response := httptest.NewRecorder()
	newTestHandler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
	}
}

func TestGetJob(t *testing.T) {
	handler := newTestHandler()
	create := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo"}`))
	handler.ServeHTTP(httptest.NewRecorder(), create)

	request := httptest.NewRequest(http.MethodGet, "/jobs/"+testJobID, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
}

func TestGetJobReturnsClientErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		id         string
		statusCode int
	}{
		{name: "malformed ID", id: "not-a-uuid", statusCode: http.StatusBadRequest},
		{name: "unknown job", id: testJobID, statusCode: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/jobs/"+test.id, nil)
			response := httptest.NewRecorder()

			newTestHandler().ServeHTTP(response, request)

			if response.Code != test.statusCode {
				t.Errorf("status = %d, want %d", response.Code, test.statusCode)
			}
		})
	}
}

func TestCreateJobDoesNotExposeRepositoryErrors(t *testing.T) {
	repository := &fakeRepository{
		jobs:      make(map[string]jobs.Job),
		nextID:    testJobID,
		createErr: errors.New("dial PostgreSQL at postgres://user:secret@host"),
	}
	service := jobs.NewService(repository, time.Now)
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"echo"}`))
	response := httptest.NewRecorder()

	NewHandler(service).ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "secret") {
		t.Errorf("response = %d %q, want sanitized server error", response.Code, response.Body.String())
	}
}
