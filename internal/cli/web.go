package cli

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Web serves a read-only inspection dashboard over HTTP. The dashboard reads
// the same SQLite store as the other commands, so it shows live queue state
// without stopping a worker. The server also exposes a small JSON API under
// /api for scripts and other tools. When -user and -pass are set, every page
// and API route requires those HTTP Basic credentials.
func Web(args []string) error {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address for the dashboard")
	dbPath := fs.String("db", "queue.db", "database path")
	user := fs.String("user", "", "username required by the dashboard (empty disables auth)")
	pass := fs.String("pass", "", "password required by the dashboard (empty disables auth)")
	fs.Parse(args)

	if (*user == "") != (*pass == "") {
		return fmt.Errorf("-user and -pass must be set together")
	}

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	opts := []web.Option{}
	if *user != "" {
		opts = append(opts, web.WithBasicAuth(*user, *pass))
	}
	srv, err := web.New(store, opts...)
	if err != nil {
		return fmt.Errorf("new dashboard: %w", err)
	}

	log.Printf("dashboard listening on %s (db=%s)", *addr, *dbPath)
	log.Printf("open the dashboard at %s", dashboardURL(*addr))
	log.Printf("scrape the JSON API with: curl -s %s/api/summary", dashboardURL(*addr))
	return http.ListenAndServe(*addr, srv.Handler())
}

// dashboardURL turns a listen address into a URL a browser can open. An
// address without a host (" :8080") becomes localhost, and an IPv6 host gains
// brackets, so the printed link is always clickable.
func dashboardURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return "http://" + host + ":" + port
}
