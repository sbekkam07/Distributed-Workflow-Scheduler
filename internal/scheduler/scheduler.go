package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Leadership is one active scheduler term. Its database session owns the
// coordination lock until Release or session loss.
type Leadership interface {
	ResolveBlocked(context.Context) (int64, error)
	QueueDepth(context.Context) (int64, error)
	Release(context.Context) error
}

// Elector tries to acquire a single scheduler leadership term.
type Elector interface {
	TryAcquire(context.Context) (Leadership, bool, error)
}

// ElectorFunc adapts a function to an Elector.
type ElectorFunc func(context.Context) (Leadership, bool, error)

// TryAcquire implements Elector.
func (f ElectorFunc) TryAcquire(ctx context.Context) (Leadership, bool, error) {
	return f(ctx)
}

// Observer receives scheduler measurements from the active leader only.
type Observer interface {
	QueueDepth(int64)
}

// Scheduler coordinates operations that must be performed by one active
// process. Job claiming remains decentralized among workers.
type Scheduler struct {
	elector      Elector
	pollInterval time.Duration
	logger       *slog.Logger
	leadership   Leadership
	observer     Observer
}

// New constructs a scheduler. It does not acquire leadership until RunOnce or
// Run, allowing multiple schedulers to start without blocking one another.
func New(elector Elector, pollInterval time.Duration, logger *slog.Logger, observers ...Observer) (*Scheduler, error) {
	if elector == nil {
		return nil, fmt.Errorf("scheduler elector is required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("scheduler poll interval must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if len(observers) > 1 {
		return nil, fmt.Errorf("at most one scheduler observer is supported")
	}
	var observer Observer
	if len(observers) == 1 {
		observer = observers[0]
	}
	return &Scheduler{elector: elector, pollInterval: pollInterval, logger: logger, observer: observer}, nil
}

// Run keeps trying for leadership and performs the leader-only reconciliation
// until ctx is canceled. A leadership/session failure is retried on the next
// polling interval; workers continue claiming independently throughout.
func (s *Scheduler) Run(ctx context.Context) error {
	defer s.Close(context.Background())
	for {
		if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
			s.logger.Error("scheduler iteration failed", "error", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		timer := time.NewTimer(s.pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

// RunOnce either obtains leadership and reconciles blocked jobs, continues an
// existing term, or returns without work when another scheduler is leader.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if s.leadership == nil {
		leadership, acquired, err := s.elector.TryAcquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire scheduler leadership: %w", err)
		}
		if !acquired {
			return nil
		}
		s.leadership = leadership
		s.logger.Info("scheduler leadership acquired")
	}

	blocked, err := s.leadership.ResolveBlocked(ctx)
	if err != nil {
		releaseErr := s.release(context.Background())
		if releaseErr != nil {
			return fmt.Errorf("reconcile blocked jobs: %w; release leadership: %v", err, releaseErr)
		}
		return fmt.Errorf("reconcile blocked jobs: %w", err)
	}
	if blocked > 0 {
		s.logger.Warn("blocked jobs with failed prerequisites", "count", blocked)
	}
	depth, err := s.leadership.QueueDepth(ctx)
	if err != nil {
		releaseErr := s.release(context.Background())
		if releaseErr != nil {
			return fmt.Errorf("read queue depth: %w; release leadership: %v", err, releaseErr)
		}
		return fmt.Errorf("read queue depth: %w", err)
	}
	if s.observer != nil {
		s.observer.QueueDepth(depth)
	}
	return nil
}

// Close releases a currently-held leadership term. It is safe to call more
// than once and is used by process shutdown and failed reconciliation paths.
func (s *Scheduler) Close(ctx context.Context) error {
	return s.release(ctx)
}

func (s *Scheduler) release(ctx context.Context) error {
	if s.leadership == nil {
		return nil
	}
	leadership := s.leadership
	s.leadership = nil
	if err := leadership.Release(ctx); err != nil {
		return err
	}
	s.logger.Info("scheduler leadership released")
	return nil
}
