// Package httpapi exposes the scheduler's HTTP interface.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/observability"
)

const maxRequestBodyBytes = 1 << 20

// NewHandler returns the job HTTP API and, when provided, a Prometheus endpoint.
func NewHandler(service *jobs.Service, metricSets ...*observability.Metrics) http.Handler {
	handler := &handler{service: service}
	if len(metricSets) > 0 {
		handler.metrics = metricSets[0]
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", handler.createJob)
	mux.HandleFunc("GET /jobs/{id}", handler.getJob)
	if handler.metrics != nil {
		mux.Handle("GET /metrics", handler.metrics.Handler())
	}
	return mux
}

type handler struct {
	service *jobs.Service
	metrics *observability.Metrics
}

type createJobRequest struct {
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
	Priority    *string         `json:"priority,omitempty"`
	RunAt       *time.Time      `json:"run_at,omitempty"`
	DependsOn   []string        `json:"depends_on,omitempty"`
}

func (h *handler) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request createJobRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must contain one JSON value")
		return
	}

	maxAttempts := jobs.DefaultMaxAttempts
	if request.MaxAttempts != nil {
		maxAttempts = *request.MaxAttempts
	}
	priority := jobs.DefaultPriority
	if request.Priority != nil {
		priority = jobs.Priority(*request.Priority)
	}
	runAt := time.Now()
	if request.RunAt != nil {
		runAt = *request.RunAt
	}
	key := r.Header.Get("Idempotency-Key")
	if key != "" {
		submission, err := h.service.SubmitIdempotently(r.Context(), request.Kind, request.Payload, maxAttempts, priority, runAt, request.DependsOn, key)
		if err != nil {
			writeCreateJobError(w, err)
			return
		}
		status := http.StatusCreated
		if !submission.Created {
			status = http.StatusOK
		} else if h.metrics != nil {
			h.metrics.JobSubmitted()
		}
		writeJSON(w, status, submission.Job)
		return
	}

	job, err := h.service.SubmitWithDependencies(r.Context(), request.Kind, request.Payload, maxAttempts, priority, runAt, request.DependsOn)
	if err != nil {
		writeCreateJobError(w, err)
		return
	}
	if h.metrics != nil {
		h.metrics.JobSubmitted()
	}
	writeJSON(w, http.StatusCreated, job)
}

func writeCreateJobError(w http.ResponseWriter, err error) {
	if err != nil {
		if errors.Is(err, jobs.ErrInvalidKind) || errors.Is(err, jobs.ErrInvalidPayload) || errors.Is(err, jobs.ErrInvalidMaxAttempts) || errors.Is(err, jobs.ErrInvalidIdempotencyKey) || errors.Is(err, jobs.ErrInvalidPriority) || errors.Is(err, jobs.ErrInvalidDependencies) || errors.Is(err, jobs.ErrDependencyNotFound) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, jobs.ErrIdempotencyConflict) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		slog.Error("create job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func (h *handler) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.service.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		switch {
		case errors.Is(err, jobs.ErrInvalidID):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, jobs.ErrNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		default:
			slog.Error("get job", "error", err)
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write JSON response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// DefaultServer configures conservative HTTP server timeouts for the API
// process. Storage calls still inherit the request context.
func DefaultServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
