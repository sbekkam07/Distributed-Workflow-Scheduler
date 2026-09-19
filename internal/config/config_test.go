package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://scheduler:secret@localhost:5432/scheduler?sslmode=disable")
	t.Setenv("DATABASE_MAX_CONNS", "7")
	t.Setenv("HTTP_ADDR", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DatabaseMaxConns != 7 {
		t.Errorf("DatabaseMaxConns = %d, want 7", cfg.DatabaseMaxConns)
	}
	if cfg.HTTPAddress != ":8080" {
		t.Errorf("HTTPAddress = %q, want %q", cfg.HTTPAddress, ":8080")
	}
	if cfg.WorkerPollInterval != defaultWorkerPollInterval {
		t.Errorf("WorkerPollInterval = %s, want %s", cfg.WorkerPollInterval, defaultWorkerPollInterval)
	}
	if cfg.WorkerLeaseDuration != defaultWorkerLeaseDuration || cfg.WorkerHeartbeatInterval != defaultWorkerHeartbeatInterval {
		t.Errorf("worker lease config = %s/%s, want %s/%s", cfg.WorkerLeaseDuration, cfg.WorkerHeartbeatInterval, defaultWorkerLeaseDuration, defaultWorkerHeartbeatInterval)
	}
	if cfg.WorkerRetryBackoffBase != defaultWorkerRetryBackoffBase || cfg.WorkerRetryBackoffMax != defaultWorkerRetryBackoffMax {
		t.Errorf("worker retry backoff config = %s/%s, want %s/%s", cfg.WorkerRetryBackoffBase, cfg.WorkerRetryBackoffMax, defaultWorkerRetryBackoffBase, defaultWorkerRetryBackoffMax)
	}
	if cfg.SchedulerPollInterval != defaultSchedulerPollInterval {
		t.Errorf("SchedulerPollInterval = %s, want %s", cfg.SchedulerPollInterval, defaultSchedulerPollInterval)
	}
}

func TestWorkerRetryBackoffConfiguration(t *testing.T) {
	base, err := workerRetryBackoffBase("2s")
	if err != nil || base != 2*time.Second {
		t.Fatalf("workerRetryBackoffBase() = %s, %v", base, err)
	}
	if _, err := workerRetryBackoffMax("1s", base); err == nil {
		t.Fatal("workerRetryBackoffMax() error = nil, want max validation error")
	}
	maximum, err := workerRetryBackoffMax("30s", base)
	if err != nil || maximum != 30*time.Second {
		t.Fatalf("workerRetryBackoffMax() = %s, %v", maximum, err)
	}
}

func TestWorkerLeaseConfiguration(t *testing.T) {
	lease, err := workerLeaseDuration("15s")
	if err != nil || lease != 15*time.Second {
		t.Fatalf("workerLeaseDuration() = %s, %v", lease, err)
	}
	if _, err := workerHeartbeatInterval("15s", lease); err == nil {
		t.Fatal("workerHeartbeatInterval() error = nil, want interval validation error")
	}
	interval, err := workerHeartbeatInterval("5s", lease)
	if err != nil || interval != 5*time.Second {
		t.Fatalf("workerHeartbeatInterval() = %s, %v", interval, err)
	}
}

func TestWorkerPollInterval(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		bad   bool
	}{
		{name: "default"},
		{name: "valid", value: "250ms"},
		{name: "zero", value: "0s", bad: true},
		{name: "invalid", value: "quickly", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := workerPollInterval(test.value)
			if (err != nil) != test.bad {
				t.Errorf("workerPollInterval(%q) error = %v, want bad = %t", test.value, err, test.bad)
			}
		})
	}
}

func TestSchedulerPollInterval(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		bad   bool
	}{
		{name: "default"},
		{name: "valid", value: "250ms"},
		{name: "zero", value: "0s", bad: true},
		{name: "invalid", value: "quickly", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := schedulerPollInterval(test.value)
			if (err != nil) != test.bad {
				t.Errorf("schedulerPollInterval(%q) error = %v, want bad = %t", test.value, err, test.bad)
			}
		})
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestDatabaseMaxConns(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  int32
		bad   bool
	}{
		{name: "default", want: defaultDatabaseMaxConns},
		{name: "positive value", value: "12", want: 12},
		{name: "zero", value: "0", bad: true},
		{name: "not a number", value: "many", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := databaseMaxConns(test.value)
			if test.bad {
				if err == nil {
					t.Fatal("databaseMaxConns() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("databaseMaxConns() error = %v", err)
			}
			if got != test.want {
				t.Errorf("databaseMaxConns() = %d, want %d", got, test.want)
			}
		})
	}
}
