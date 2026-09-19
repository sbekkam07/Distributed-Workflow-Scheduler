package observability

import (
	"net/http"
	"time"
)

// NewServer returns a conservative HTTP server for a process metrics endpoint.
func NewServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
