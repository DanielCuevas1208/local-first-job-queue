package cli

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Web serves a read-only HTML dashboard for queue inspection. The dashboard
// shows state counts, jobs, and recent events, and each job page shows its full
// event timeline. Open the printed address in a browser.
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

	log.Printf("dashboard listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("open the dashboard at http://localhost%s", *addr)
	return http.ListenAndServe(*addr, web.Handler(store))
}
