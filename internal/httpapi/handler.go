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
)

const maxRequestBodyBytes = 1 << 20

// NewHandler returns the Phase 1 job HTTP API.
func NewHandler(service *jobs.Service) http.Handler {
	handler := &handler{service: service}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", handler.createJob)
	mux.HandleFunc("GET /jobs/{id}", handler.getJob)
	return mux
}

type handler struct {
	service *jobs.Service
}

type createJobRequest struct {
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
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
	job, err := h.service.SubmitWithMaxAttempts(r.Context(), request.Kind, request.Payload, maxAttempts)
	if err != nil {
		if errors.Is(err, jobs.ErrInvalidKind) || errors.Is(err, jobs.ErrInvalidPayload) || errors.Is(err, jobs.ErrInvalidMaxAttempts) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		slog.Error("create job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusCreated, job)
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
