package cli

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/local-first-job-queue/internal/metrics"
	"github.com/local-first-job-queue/internal/queue"
)

// Metrics exposes the queue state in the Prometheus text exposition format.
// With -once it prints a snapshot and exits, which suits scripts and the demo.
// Without -once it serves an HTTP endpoint that recomputes the snapshot on
// every scrape, so a Prometheus server can poll the running queue.
func Metrics(args []string) error {
	fs := flag.NewFlagSet("metrics", flag.ExitOnError)
	addr := fs.String("addr", ":9090", "listen address for the HTTP endpoint")
	dbPath := fs.String("db", "queue.db", "database path")
	once := fs.Bool("once", false, "print one snapshot and exit")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	if *once {
		return renderMetrics(os.Stdout, store)
	}

	log.Printf("metrics listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("scrape with: curl -s %s/metrics", *addr)
	return http.ListenAndServe(*addr, metrics.Handler(store))
}

// renderMetrics writes one queue snapshot in the Prometheus text format to w.
// It is separate from Metrics so tests can capture the output without serving.
func renderMetrics(w io.Writer, store *queue.SQLiteStore) error {
	if err := metrics.New(store).Write(w); err != nil {
		return fmt.Errorf("render metrics: %w", err)
	}
	return nil
}
