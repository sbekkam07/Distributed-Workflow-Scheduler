// Package jobs contains the job domain model and use cases.
//
// Keeping scheduling rules here makes them independent of HTTP and PostgreSQL, so
// they can be tested without running a server or database.
package jobs
