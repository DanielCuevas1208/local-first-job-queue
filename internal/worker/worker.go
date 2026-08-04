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

	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	running bool
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
		if ackErr := w.queue.FailLease(job.ID, job.LeasedUntil, err.Error()); ackErr != nil {
			log.Printf("fail error for %s: %v", job.ID, ackErr)
		}
		return
	}

	if err := w.queue.AcknowledgeLease(job.ID, job.LeasedUntil); err != nil {
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

func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		w.cancel()
	}
}
