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
	defaultDatabaseMaxConns   int32 = 4
	defaultWorkerPollInterval       = 500 * time.Millisecond
)

// Config is the configuration shared by the API and worker processes.
type Config struct {
	DatabaseURL        string
	DatabaseMaxConns   int32
	HTTPAddress        string
	WorkerPollInterval time.Duration
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

	return Config{
		DatabaseURL:        databaseURL,
		DatabaseMaxConns:   maxConns,
		HTTPAddress:        httpAddress,
		WorkerPollInterval: workerPollInterval,
	}, nil
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
