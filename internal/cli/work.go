package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/local-first-job-queue/internal/fault"
	"github.com/local-first-job-queue/internal/metrics"
	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/web"
	"github.com/local-first-job-queue/internal/worker"
)

type echoHandler struct{}

func (echoHandler) Handle(ctx context.Context, job queue.Job) error {
	log.Printf("processed kind=%s id=%s payload=%s", job.Kind, job.ID, job.Payload)
	return nil
}

// Work starts a worker process for one job kind. On startup it recovers
// orphaned leases left by previous workers. A payload may carry a "fault"
// object; the fault injector honors it so a real worker run can reproduce
// retries, panics, and crash recovery without extra tooling.
func Work(args []string) error {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	kind := fs.String("kind", "default", "job kind to process")
	dbPath := fs.String("db", "queue.db", "database path")
	concurrency := fs.Int("concurrency", 1, "number of concurrent workers")
	leaseDuration := fs.Duration("lease", 30*time.Second, "lease duration per job")
	pollInterval := fs.Duration("poll", time.Second, "time between lease attempts")
	aging := fs.Duration("aging", queue.DefaultAgingInterval, "priority aging interval; a job gains one priority point per interval it waits (0 disables)")
	metricsAddr := fs.String("metrics-addr", "", "address to serve Prometheus metrics on, e.g. :9090 (empty disables)")
	webAddr := fs.String("web-addr", "", "address to serve the inspection dashboard on, e.g. :8080 (empty disables)")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath, queue.WithAgingInterval(*aging))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	handler := fault.New(echoHandler{}.Handle, fault.FromPayload).Handle
	w := worker.NewWorker(q, handler, *kind,
		worker.WithConcurrency(*concurrency),
		worker.WithLeaseDuration(*leaseDuration),
		worker.WithPollInterval(*pollInterval),
	)

	var servers []*http.Server
	serve := func(addr string, h http.Handler, label string) {
		if addr == "" {
			return
		}
		srv := &http.Server{Addr: addr, Handler: h}
		servers = append(servers, srv)
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("%s: %v", label, err)
			}
		}()
		log.Printf("%s listening on %s", label, addr)
	}
	serve(*metricsAddr, metrics.Handler(store), "metrics")
	if *webAddr != "" {
		serve(*webAddr, web.New(store, web.WithDBPath(*dbPath)).Handler(), "dashboard")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker started kind=%q concurrency=%d lease=%s poll=%s aging=%s",
		*kind, *concurrency, *leaseDuration, *pollInterval, *aging)
	err = w.Run(ctx)

	for _, srv := range servers {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = srv.Shutdown(shutdownCtx)
		cancel()
	}
	if errors.Is(err, context.Canceled) {
		// A signal cancelled the run. This is a normal shutdown, not an error.
		return nil
	}
	return err
}
