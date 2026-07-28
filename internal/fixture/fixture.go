package fixture

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/worker"
)

type WorkloadType string

const (
	TaskEmail   WorkloadType = "email"
	TaskReport  WorkloadType = "report"
	TaskCleanup WorkloadType = "cleanup"
	TaskBackup  WorkloadType = "backup"
	TaskNotify  WorkloadType = "notify"
)

var Workloads = []WorkloadType{TaskEmail, TaskReport, TaskCleanup, TaskBackup, TaskNotify}

// LoadSampleData enqueues three jobs of each workload kind. Each job carries an
// idempotency key built from its kind and index, so repeated calls do not add
// duplicates. The returned slice reports every attempt that was made.
func LoadSampleData(q *queue.Queue) ([]*queue.Job, error) {
	now := time.Now()
	var jobs []*queue.Job
	for _, wt := range Workloads {
		for i := 0; i < 3; i++ {
			payload := map[string]any{
				"task": string(wt),
				"seq":  i,
				"time": now.Format(time.RFC3339),
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("marshal payload %s-%d: %w", wt, i, err)
			}
			key := fmt.Sprintf("sample:%s:%d", wt, i)
			job, err := q.Enqueue(string(wt), string(raw),
				queue.WithIdempotencyKey(key),
			)
			if err != nil {
				return nil, fmt.Errorf("enqueue %s-%d: %w", wt, i, err)
			}
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

// SampleHandler is kept for tests that want a randomized handler. It sleeps a
// short, bounded time and fails with a small probability. Use the fault package
// for deterministic scenarios instead.
func SampleHandler(shouldFail bool) worker.JobHandler {
	return func(ctx context.Context, job queue.Job) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(50+rand.Intn(200)) * time.Millisecond):
		}
		if shouldFail && rand.Float32() < 0.3 {
			return fmt.Errorf("simulated failure for %s", job.ID)
		}
		return nil
	}
}
