package cli

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Web starts a local browser dashboard for queue inspection. The server serves
// the dashboard, a JSON API, and the Prometheus metrics endpoint on one port.
// The dashboard reads the same SQLite store as every other command, so it
// shows live state without any extra pipeline.
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

	log.Printf("web dashboard listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("open http://localhost%s/ in a browser", *addr)
	log.Printf("scrape metrics with: curl -s %s/metrics", *addr)
	return http.ListenAndServe(*addr, web.New(store))
}
