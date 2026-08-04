package cli

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Web serves a local dashboard for queue inspection. The page reads the same
// SQLite store as the other commands, so it shows the current queue state and
// event log. An operator can requeue a dead-lettered job from the page.
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

	handler := web.Handler(store, web.WithDBPath(*dbPath))
	log.Printf("dashboard: open http://localhost%s", *addr)
	log.Printf("metrics:   scrape http://localhost%s/metrics", *addr)
	return http.ListenAndServe(*addr, handler)
}
