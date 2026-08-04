package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

type JobHandler func(ctx context.Context, job queue.Job) error

type Worker struct {
	queue         *queue.Queue
	handler       JobHandler
	pollInterval  time.Duration
	leaseDuration time.Duration
	kind          string
	concurrency   int
	retention     *retentionSpec

	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	running bool
}

// retentionSpec holds the policy and cadence of the automatic retention runs
// that a worker performs beside its job loop.
type retentionSpec struct {
	policy   queue.PrunePolicy
	interval time.Duration
}

type WorkerOption func(*Worker)

func WithPollInterval(d time.Duration) WorkerOption {
	return func(w *Worker) {
		w.pollInterval = d
	}
}

func WithLeaseDuration(d time.Duration) WorkerOption {
	return func(w *Worker) {
		w.leaseDuration = d
	}
}

func WithConcurrency(n int) WorkerOption {
	return func(w *Worker) {
		w.concurrency = n
	}
}

// WithRetention enables scheduled auto-retention in the worker. The worker
// applies policy to the shared store once at startup and then on every
// interval, removing old terminal jobs and capping the event log while it
// continues to process work. A zero policy removes nothing, so an operator can
// configure the loop before choosing limits. Prune is transactional and
// idempotent, so several workers that run the same policy stay safe on one
// file.
func WithRetention(policy queue.PrunePolicy, interval time.Duration) WorkerOption {
	return func(w *Worker) {
		w.retention = &retentionSpec{policy: policy, interval: interval}
	}
}

func NewWorker(q *queue.Queue, handler JobHandler, kind string, opts ...WorkerOption) *Worker {
	w := &Worker{
		queue:         q,
		handler:       handler,
		kind:          kind,
		pollInterval:  time.Second,
		leaseDuration: 30 * time.Second,
		concurrency:   1,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("worker already running")
	}
	w.running = true
	ctx, w.cancel = context.WithCancel(ctx)
	w.mu.Unlock()

	recovered, err := w.queue.Recover()
	if err != nil {
		log.Printf("recovery: %v", err)
	} else if recovered > 0 {
		log.Printf("recovered %d orphaned jobs", recovered)
	}

	sem := make(chan struct{}, w.concurrency)
	for i := 0; i < w.concurrency; i++ {
		sem <- struct{}{}
	}

	if w.retention != nil {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.retentionLoop(ctx)
		}()
	}

	for {
		select {
		case <-ctx.Done():
			w.wg.Wait()
			return ctx.Err()
		case <-sem:
			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				defer func() { sem <- struct{}{} }()
				w.processOne(ctx)
			}()
		}
	}
}

func (w *Worker) processOne(ctx context.Context) {
	timer := time.NewTimer(w.pollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	job, err := w.queue.Lease(ctx, w.kind, w.leaseDuration)
	if err != nil {
		log.Printf("lease error: %v", err)
		return
	}
	if job == nil {
		return
	}

	runCtx, cancel := context.WithTimeout(ctx, w.leaseDuration)
	defer cancel()

	err = w.runHandler(runCtx, *job)
	if err != nil {
		log.Printf("job %s failed: %v", job.ID, err)
		if ackErr := w.queue.Fail(job.ID, err.Error()); ackErr != nil {
			log.Printf("fail error for %s: %v", job.ID, ackErr)
		}
		return
	}

	if err := w.queue.Acknowledge(job.ID); err != nil {
		log.Printf("ack error for %s: %v", job.ID, err)
	}
}

// runHandler invokes the handler and converts a panic into an error. This keeps
// the worker process alive when a handler panics and lets the job retry.
func (w *Worker) runHandler(ctx context.Context, job queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return w.handler(ctx, job)
}

// retentionLoop applies the retention policy once at startup and then on a
// fixed interval until the worker context ends. The store serializes writers,
// so a prune transaction never interleaves with a lease claim or another
// worker's retention run.
func (w *Worker) retentionLoop(ctx context.Context) {
	w.runRetention()
	interval := w.retention.interval
	if interval <= 0 {
		return
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			w.runRetention()
			timer.Reset(interval)
		}
	}
}

// runRetention performs one prune pass and reports what it removed. Errors are
// logged and skipped, so a transient store failure does not stop the worker.
func (w *Worker) runRetention() {
	res, err := w.queue.Prune(w.retention.policy)
	if err != nil {
		log.Printf("auto-retention: %v", err)
		return
	}
	if res.JobsRemoved > 0 || res.EventsRemoved > 0 {
		log.Printf("auto-retention: removed %d job(s) and %d event(s)", res.JobsRemoved, res.EventsRemoved)
	}
}

func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		w.cancel()
	}
}
