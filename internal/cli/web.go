package cli

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Web serves the HTML inspection dashboard and its JSON API. The dashboard
// reads the same SQLite store as the worker, so it shows jobs and events as
// they change. Point a browser at the listen address and open the root path.
func Web(args []string) error {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address for the dashboard")
	dbPath := fs.String("db", "queue.db", "database path")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	handler := web.New(store, web.WithDBPath(*dbPath)).Handler()

	log.Printf("dashboard listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("open: http://localhost%s/", *addr)
	return http.ListenAndServe(*addr, handler)
}
