package worker

import (
	"context"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// TestWorkerAutoRetentionRecordsActivity verifies that a scheduled worker pass
// records its source and removal counts for later inspection.
func TestWorkerAutoRetentionRecordsActivity(t *testing.T) {
	q, s := newTestQueue(t)
	job, err := q.Enqueue("test", `{}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	completeJob(t, q, s, job.ID)
	ageJob(t, s, job.ID, time.Now().UTC().Add(-48*time.Hour))

	w := NewWorker(q, func(context.Context, queue.Job) error { return nil }, "test",
		WithPollInterval(10*time.Millisecond),
		WithLeaseDuration(time.Minute),
		WithRetention(queue.PrunePolicy{MaxJobAge: time.Hour}, 20*time.Millisecond),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()

	waitFor(t, func() bool {
		stats, err := s.GetRetentionStats()
		if err != nil || len(stats) != 2 {
			return false
		}
		return stats[1].Runs > 0 && stats[1].JobsRemoved == 1
	}, 2*time.Second)
	cancel()
	<-done

	stats, err := s.GetRetentionStats()
	if err != nil {
		t.Fatalf("retention stats: %v", err)
	}
	if stats[1].Source != queue.RetentionSourceAuto || stats[1].JobsRemoved != 1 {
		t.Fatalf("expected automatic removal stats, got %+v", stats)
	}
}
