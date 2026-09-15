// Command migrate applies the embedded PostgreSQL schema migrations.
package main

import (
	"context"
	"log"
	"time"

	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/config"
	"github.com/sohanbekkam/distributed-workflow-scheduler/internal/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := postgres.MigrateUp(ctx, cfg.DatabaseURL); err != nil {
		log.Fatalf("apply migrations: %v", err)
	}

	log.Println("database migrations are current")
}
