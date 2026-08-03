package cli

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/dashboard"
	"github.com/local-first-job-queue/internal/queue"
)

// Web serves the read-only inspection dashboard. The dashboard renders the
// queue state as one HTML page and exposes the same state as JSON endpoints.
// It never writes to the queue, so it is safe to leave running beside a worker.
func Web(args []string) error {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address for the dashboard")
	dbPath := fs.String("db", "queue.db", "database path")
	refresh := fs.Duration("refresh", dashboard.DefaultRefreshInterval, "client auto-refresh interval")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	log.Printf("dashboard listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("open http://localhost%s", *addr)
	return http.ListenAndServe(*addr, dashboard.Handler(store,
		dashboard.WithDBPath(*dbPath),
		dashboard.WithRefreshInterval(*refresh),
	))
}
