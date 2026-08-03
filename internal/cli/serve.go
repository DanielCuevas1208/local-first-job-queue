package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
)

// Serve starts the web dashboard for queue inspection. The server embeds its
// templates and styles, so the binary is self-contained. One endpoint also
// exposes Prometheus metrics at /metrics, matching the metrics command. The
// process shuts down cleanly on SIGINT or SIGTERM.
func Serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address for the web dashboard")
	dbPath := fs.String("db", "queue.db", "database path")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	srv := &http.Server{Addr: *addr, Handler: web.Handler(store)}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("web dashboard: %s (db=%s)", displayURL(*addr), *dbPath)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	}
}

// displayURL turns a listen address into a browser-friendly URL. An address
// that binds every interface is shown as localhost so the printed link opens
// the dashboard on the same machine.
func displayURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}
	return addr
}
