package cli

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Web serves the read-only browser interface for a queue database. The pages
// render fresh snapshots from SQLite, so an operator can watch a live worker
// from the dashboard without an extra collection pipeline. The interface never
// mutates the queue; mutations stay in the other commands.
func Web(args []string) error {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address for the web interface")
	dbPath := fs.String("db", "queue.db", "database path")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	srv, err := web.New(store)
	if err != nil {
		return fmt.Errorf("prepare web interface: %w", err)
	}

	log.Printf("web interface listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("open with: http://localhost%s", *addr)
	return http.ListenAndServe(*addr, srv.Handler())
}
