package migrations

import (
	"io/fs"
	"testing"
)

func TestFSEmbedsInitialMigrationPair(t *testing.T) {
	for _, name := range []string{
		"000001_create_jobs.up.sql",
		"000001_create_jobs.down.sql",
		"000002_add_queued_claim_index.up.sql",
		"000002_add_queued_claim_index.down.sql",
		"000003_add_job_leases.up.sql",
		"000003_add_job_leases.down.sql",
		"000004_add_job_retries.up.sql",
		"000004_add_job_retries.down.sql",
	} {
		contents, err := fs.ReadFile(FS, name)
		if err != nil {
			t.Fatalf("embedded migration %q: %v", name, err)
		}
		if len(contents) == 0 {
			t.Errorf("embedded migration %q is empty", name)
		}
	}
}
