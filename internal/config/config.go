// Package config loads and validates process configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDatabaseMaxConns        int32 = 4
	defaultWorkerPollInterval            = 500 * time.Millisecond
	defaultWorkerLeaseDuration           = 10 * time.Second
	defaultWorkerHeartbeatInterval       = 3 * time.Second
	defaultWorkerRetryBackoffBase        = time.Second
	defaultWorkerRetryBackoffMax         = time.Minute
)

// Config is the configuration shared by the API and worker processes.
type Config struct {
	DatabaseURL             string
	DatabaseMaxConns        int32
	HTTPAddress             string
	WorkerPollInterval      time.Duration
	WorkerLeaseDuration     time.Duration
	WorkerHeartbeatInterval time.Duration
	WorkerRetryBackoffBase  time.Duration
	WorkerRetryBackoffMax   time.Duration
	WorkerID                string
}

// Load reads configuration from environment variables.
//
// DATABASE_URL must be a PostgreSQL connection URL. DATABASE_MAX_CONNS is
// optional and defaults to a deliberately small local-development pool.
func Load() (Config, error) {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	maxConns, err := databaseMaxConns(os.Getenv("DATABASE_MAX_CONNS"))
	if err != nil {
		return Config{}, err
	}

	httpAddress := strings.TrimSpace(os.Getenv("HTTP_ADDR"))
	if httpAddress == "" {
		httpAddress = ":8080"
	}
	workerPollInterval, err := workerPollInterval(os.Getenv("WORKER_POLL_INTERVAL"))
	if err != nil {
		return Config{}, err
	}
	workerLeaseDuration, err := workerLeaseDuration(os.Getenv("WORKER_LEASE_DURATION"))
	if err != nil {
		return Config{}, err
	}
	workerHeartbeatInterval, err := workerHeartbeatInterval(os.Getenv("WORKER_HEARTBEAT_INTERVAL"), workerLeaseDuration)
	if err != nil {
		return Config{}, err
	}
	workerRetryBackoffBase, err := workerRetryBackoffBase(os.Getenv("WORKER_RETRY_BACKOFF_BASE"))
	if err != nil {
		return Config{}, err
	}
	workerRetryBackoffMax, err := workerRetryBackoffMax(os.Getenv("WORKER_RETRY_BACKOFF_MAX"), workerRetryBackoffBase)
	if err != nil {
		return Config{}, err
	}

	return Config{
		DatabaseURL:             databaseURL,
		DatabaseMaxConns:        maxConns,
		HTTPAddress:             httpAddress,
		WorkerPollInterval:      workerPollInterval,
		WorkerLeaseDuration:     workerLeaseDuration,
		WorkerHeartbeatInterval: workerHeartbeatInterval,
		WorkerRetryBackoffBase:  workerRetryBackoffBase,
		WorkerRetryBackoffMax:   workerRetryBackoffMax,
		WorkerID:                strings.TrimSpace(os.Getenv("WORKER_ID")),
	}, nil
}

func workerRetryBackoffBase(value string) (time.Duration, error) {
	return positiveDuration("WORKER_RETRY_BACKOFF_BASE", value, defaultWorkerRetryBackoffBase)
}

func workerRetryBackoffMax(value string, base time.Duration) (time.Duration, error) {
	maximum, err := positiveDuration("WORKER_RETRY_BACKOFF_MAX", value, defaultWorkerRetryBackoffMax)
	if err != nil {
		return 0, err
	}
	if maximum < base {
		return 0, fmt.Errorf("WORKER_RETRY_BACKOFF_MAX must be at least WORKER_RETRY_BACKOFF_BASE")
	}
	return maximum, nil
}

func workerLeaseDuration(value string) (time.Duration, error) {
	return positiveDuration("WORKER_LEASE_DURATION", value, defaultWorkerLeaseDuration)
}

func workerHeartbeatInterval(value string, leaseDuration time.Duration) (time.Duration, error) {
	interval, err := positiveDuration("WORKER_HEARTBEAT_INTERVAL", value, defaultWorkerHeartbeatInterval)
	if err != nil {
		return 0, err
	}
	if interval >= leaseDuration {
		return 0, fmt.Errorf("WORKER_HEARTBEAT_INTERVAL must be shorter than WORKER_LEASE_DURATION")
	}
	return interval, nil
}

func positiveDuration(name, value string, defaultValue time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return defaultValue, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}

func workerPollInterval(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return defaultWorkerPollInterval, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("WORKER_POLL_INTERVAL must be a positive duration")
	}
	return parsed, nil
}

func databaseMaxConns(value string) (int32, error) {
	if strings.TrimSpace(value) == "" {
		return defaultDatabaseMaxConns, nil
	}

	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("DATABASE_MAX_CONNS must be a positive integer")
	}
	return int32(parsed), nil
}
