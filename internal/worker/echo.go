package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

// EchoExecutor is Phase 1's deterministic executor. It logs the supplied
// message and has no external side effect.
type EchoExecutor struct {
	logger *slog.Logger
}

// NewEchoExecutor constructs the echo executor used by the first worker.
func NewEchoExecutor(logger *slog.Logger) *EchoExecutor {
	if logger == nil {
		logger = slog.Default()
	}
	return &EchoExecutor{logger: logger}
}

// Execute validates and logs an echo job.
func (e *EchoExecutor) Execute(ctx context.Context, job jobs.Job) error {
	if job.Kind != "echo" {
		return fmt.Errorf("unsupported job kind %q", job.Kind)
	}

	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode echo payload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	e.logger.Info("echo job", "job_id", job.ID, "effect_key", job.EffectKey(), "message", payload.Message)
	return nil
}
