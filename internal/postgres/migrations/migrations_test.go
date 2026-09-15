package migrations

import (
	"io/fs"
	"testing"
)

func TestFSEmbedsInitialMigrationPair(t *testing.T) {
	for _, name := range []string{
		"000001_create_jobs.up.sql",
		"000001_create_jobs.down.sql",
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
