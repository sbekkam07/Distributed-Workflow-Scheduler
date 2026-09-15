// Command api will expose the workflow scheduler's HTTP API.
//
// In Phase 1 it will own the POST /jobs and GET /jobs/{id} endpoints.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/config"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/httpapi"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/jobs"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.OpenPool(ctx, cfg.DatabaseURL, cfg.DatabaseMaxConns)
	if err != nil {
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	repository := postgres.NewJobRepository(pool)
	service := jobs.NewService(repository, time.Now)
	server := httpapi.DefaultServer(cfg.HTTPAddress, httpapi.NewHandler(service))

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	log.Printf("API listening on %s", cfg.HTTPAddress)
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("API server failed: %v", err)
		}
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Printf("API shutdown: %v", err)
		}
		log.Println("API shut down")
	}
}
