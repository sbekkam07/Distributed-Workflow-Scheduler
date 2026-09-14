// Package httpapi contains HTTP routing, request validation, and response handling.
//
// It must not contain job-state transition or database-query logic; those belong to
// the jobs and postgres packages respectively.
package httpapi
