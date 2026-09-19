package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeElector struct {
	active       bool
	acquisitions int
	resolutions  int
	resolveErr   error
}

func (e *fakeElector) TryAcquire(context.Context) (Leadership, bool, error) {
	if e.active {
		return nil, false, nil
	}
	e.active = true
	e.acquisitions++
	return &fakeLeadership{elector: e}, true, nil
}

type fakeLeadership struct {
	elector *fakeElector
}

func (l *fakeLeadership) ResolveBlocked(context.Context) (int64, error) {
	l.elector.resolutions++
	return 0, l.elector.resolveErr
}

func (l *fakeLeadership) Release(context.Context) error {
	l.elector.active = false
	return nil
}

func TestRunOnceKeepsOneLeaderAndFailsOver(t *testing.T) {
	elector := &fakeElector{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := New(elector, time.Second, logger)
	if err != nil {
		t.Fatalf("New first scheduler: %v", err)
	}
	second, err := New(elector, time.Second, logger)
	if err != nil {
		t.Fatalf("New second scheduler: %v", err)
	}

	if err := first.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	if err := second.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce while first leads: %v", err)
	}
	if elector.acquisitions != 1 || elector.resolutions != 1 {
		t.Fatalf("leadership activity = %d acquisitions, %d resolutions; want 1, 1", elector.acquisitions, elector.resolutions)
	}

	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := second.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce after failover: %v", err)
	}
	if elector.acquisitions != 2 || elector.resolutions != 2 {
		t.Errorf("leadership activity = %d acquisitions, %d resolutions; want 2, 2", elector.acquisitions, elector.resolutions)
	}
}

func TestRunOnceReleasesLeadershipAfterReconciliationError(t *testing.T) {
	elector := &fakeElector{resolveErr: errors.New("database connection lost")}
	s, err := New(elector, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New scheduler: %v", err)
	}
	if err := s.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce error = nil, want reconciliation error")
	}
	if elector.active {
		t.Error("leadership remained active after reconciliation error")
	}
}

func TestNewValidatesConfiguration(t *testing.T) {
	if _, err := New(nil, time.Second, nil); err == nil {
		t.Fatal("New(nil elector) error = nil, want validation error")
	}
	if _, err := New(&fakeElector{}, 0, nil); err == nil {
		t.Fatal("New(zero poll interval) error = nil, want validation error")
	}
}
