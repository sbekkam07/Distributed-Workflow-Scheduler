// Command loadtest submits echo jobs and reports durable scheduler latency.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/loadtest"
)

type options struct {
	apiURL      string
	jobs        int
	concurrency int
	timeout     time.Duration
	poll        time.Duration
}

type apiJob struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

func main() {
	options := options{}
	flag.StringVar(&options.apiURL, "api-url", "http://localhost:8080", "scheduler API base URL")
	flag.IntVar(&options.jobs, "jobs", 100, "number of echo jobs to submit")
	flag.IntVar(&options.concurrency, "concurrency", 8, "maximum simultaneous API requests")
	flag.DurationVar(&options.timeout, "timeout", 2*time.Minute, "whole-run timeout")
	flag.DurationVar(&options.poll, "poll-interval", 100*time.Millisecond, "job status polling interval")
	flag.Parse()
	if options.jobs < 1 || options.concurrency < 1 || options.timeout <= 0 || options.poll <= 0 {
		fmt.Fprintln(os.Stderr, "jobs, concurrency, timeout, and poll-interval must be positive")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.timeout)
	defer cancel()
	report, err := run(ctx, options, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintln(os.Stderr, "load test failed:", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, options options, client *http.Client) (loadtest.Report, error) {
	started := time.Now()
	baseURL := strings.TrimRight(options.apiURL, "/")
	created, err := submitJobs(ctx, client, baseURL, options.jobs, options.concurrency)
	if err != nil {
		return loadtest.Report{}, err
	}
	samples, err := waitForTerminal(ctx, client, baseURL, created, options.concurrency, options.poll)
	if err != nil {
		return loadtest.Report{}, err
	}
	return loadtest.Summarize(options.jobs, started, time.Now(), samples), nil
}

func submitJobs(ctx context.Context, client *http.Client, baseURL string, count, concurrency int) ([]apiJob, error) {
	jobs := make(chan int)
	created := make([]apiJob, 0, count)
	var mutex sync.Mutex
	var firstErr error
	var group sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				mutex.Lock()
				failed := firstErr != nil
				mutex.Unlock()
				if failed {
					continue
				}
				job, err := submitOne(ctx, client, baseURL, index)
				mutex.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				if err == nil {
					created = append(created, job)
				}
				mutex.Unlock()
			}
		}()
	}
	for index := 0; index < count; index++ {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return created, nil
}

func submitOne(ctx context.Context, client *http.Client, baseURL string, index int) (apiJob, error) {
	body, err := json.Marshal(map[string]any{"kind": "echo", "payload": map[string]string{"message": fmt.Sprintf("loadtest-%d", index)}})
	if err != nil {
		return apiJob{}, fmt.Errorf("encode job: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/jobs", bytes.NewReader(body))
	if err != nil {
		return apiJob{}, fmt.Errorf("create submit request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return apiJob{}, fmt.Errorf("submit job: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return apiJob{}, fmt.Errorf("submit job: status %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	var job apiJob
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		return apiJob{}, fmt.Errorf("decode submitted job: %w", err)
	}
	return job, nil
}

func waitForTerminal(ctx context.Context, client *http.Client, baseURL string, jobs []apiJob, concurrency int, interval time.Duration) ([]loadtest.Sample, error) {
	pending := make(chan apiJob)
	samples := make([]loadtest.Sample, 0, len(jobs))
	var mutex sync.Mutex
	var firstErr error
	var group sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for job := range pending {
				sample, err := waitOne(ctx, client, baseURL, job, interval)
				mutex.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				if err == nil {
					samples = append(samples, sample)
				}
				mutex.Unlock()
			}
		}()
	}
	for _, job := range jobs {
		pending <- job
	}
	close(pending)
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return samples, nil
}

func waitOne(ctx context.Context, client *http.Client, baseURL string, job apiJob, interval time.Duration) (loadtest.Sample, error) {
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/jobs/"+job.ID, nil)
		if err != nil {
			return loadtest.Sample{}, fmt.Errorf("create get request: %w", err)
		}
		response, err := client.Do(request)
		if err != nil {
			return loadtest.Sample{}, fmt.Errorf("get job %s: %w", job.ID, err)
		}
		var current apiJob
		decodeErr := json.NewDecoder(response.Body).Decode(&current)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return loadtest.Sample{}, fmt.Errorf("get job %s: status %d", job.ID, response.StatusCode)
		}
		if decodeErr != nil {
			return loadtest.Sample{}, fmt.Errorf("decode job %s: %w", job.ID, decodeErr)
		}
		if terminal(current.Status) {
			return loadtest.Sample{Status: current.Status, CreatedAt: current.CreatedAt, StartedAt: current.StartedAt, CompletedAt: current.CompletedAt}, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return loadtest.Sample{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func terminal(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "DEAD", "BLOCKED":
		return true
	default:
		return false
	}
}
