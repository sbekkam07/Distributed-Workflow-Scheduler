// Package migrations exposes the versioned database schema embedded in the
// migrate executable.
package migrations

import "embed"

// FS contains the ordered up and down migration files.
//
//go:embed *.sql
var FS embed.FS
