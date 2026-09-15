package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

func TestEchoExecutor(t *testing.T) {
	executor := NewEchoExecutor(slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := executor.Execute(context.Background(), jobs.Job{
		ID:      "job-1",
		Kind:    "echo",
		Payload: []byte(`{"message":"hello"}`),
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestEchoExecutorRejectsUnsupportedKind(t *testing.T) {
	executor := NewEchoExecutor(nil)

	if err := executor.Execute(context.Background(), jobs.Job{Kind: "http", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("Execute() error = nil, want error")
	}
}
