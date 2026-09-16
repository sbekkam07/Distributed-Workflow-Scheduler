package worker

import (
	"context"
	"errors"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
)

// Retryable wraps an execution error to request a later retry. Executors use
// it only for failures that may succeed when run again.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return retryableError{cause: err}
}

type retryableError struct {
	cause error
}

func (e retryableError) Error() string { return e.cause.Error() }

func (e retryableError) Unwrap() error { return e.cause }

func isRetryable(err error) bool {
	if errors.Is(err, jobs.ErrLeaseLost) {
		return false
	}
	var marked retryableError
	return errors.As(err, &marked) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func exponentialBackoff(base, maximum time.Duration, attempt int) time.Duration {
	backoff := base
	for retry := 1; retry < attempt && backoff < maximum; retry++ {
		if backoff > maximum/2 {
			return maximum
		}
		backoff *= 2
	}
	if backoff > maximum {
		return maximum
	}
	return backoff
}
