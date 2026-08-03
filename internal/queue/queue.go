package queue

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultMaxAttempts is the maximum number of attempts allowed for a job,
// including the first attempt. A job with this value runs once and may retry
// twice more before it enters the dead-letter state.
const DefaultMaxAttempts = 3

// DefaultPriority is used when an enqueue call does not set a priority.
const DefaultPriority = 0

type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	idempotencyKey string
	maxAttempts    int
	priority       int
	runAt          *time.Time
}

// WithIdempotencyKey makes Enqueue return the existing job when a job with the
// same key already exists. Duplicate enqueues with the same key are no-ops.
func WithIdempotencyKey(key string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.idempotencyKey = key
	}
}

// WithMaxAttempts sets the maximum number of attempts allowed for the job,
// including the first attempt. Values below one are clamped to one.
func WithMaxAttempts(n int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.maxAttempts = n
	}
}

// WithPriority sets the job priority. Higher values are leased before lower values.
func WithPriority(priority int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.priority = priority
	}
}

// WithRunAt delays leasing of the job until the supplied time. A zero value is
// treated as no delay. Use WithRunAfter for a relative delay from now.
func WithRunAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) {
		if t.IsZero() {
			c.runAt = nil
			return
		}
		v := t.UTC()
		c.runAt = &v
	}
}

// WithRunAfter delays leasing of the job by d relative to the enqueue moment.
// A non-positive duration means the job is ready immediately.
func WithRunAfter(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		if d <= 0 {
			c.runAt = nil
			return
		}
		v := time.Now().UTC().Add(d)
		c.runAt = &v
	}
}

type RequeueOption func(*requeueConfig)

type requeueConfig struct {
	maxAttempts int
	payload     *string
}

// RequeueWithMaxAttempts sets a new attempt budget for a requeued job. The
// default keeps the budget the job had when it was dead-lettered.
func RequeueWithMaxAttempts(n int) RequeueOption {
	return func(c *requeueConfig) {
		c.maxAttempts = n
	}
}

// RequeueWithPayload replaces the payload of a requeued job. The default keeps
// the original payload, so an operator can fix the job data before retrying.
func RequeueWithPayload(payload string) RequeueOption {
	return func(c *requeueConfig) {
		v := payload
		c.payload = &v
	}
}

type Queue struct {
	store *SQLiteStore
}

func NewQueue(store *SQLiteStore) *Queue {
	return &Queue{store: store}
}

func (q *Queue) Enqueue(kind, payload string, opts ...EnqueueOption) (*Job, error) {
	cfg := enqueueConfig{maxAttempts: DefaultMaxAttempts, priority: DefaultPriority}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.maxAttempts < 1 {
		cfg.maxAttempts = 1
	}

	id, err := newID()
	if err != nil {
		return nil, fmt.Errorf("new id: %w", err)
	}

	now := time.Now().UTC()
	job := Job{
		ID:          id,
		Kind:        kind,
		Payload:     payload,
		State:       StatePending,
		MaxAttempts: cfg.maxAttempts,
		Priority:    cfg.priority,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if cfg.idempotencyKey != "" {
		key := cfg.idempotencyKey
		job.IdempotencyKey = &key
	}
	if cfg.runAt != nil {
		job.RunAt = cfg.runAt
	}

	inserted, err := q.store.InsertJob(job)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	if !inserted {
		// A job with the same idempotency key already exists. Return it.
		existing, err := q.store.FindJobByIdempotencyKey(cfg.idempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("lookup idempotent job: %w", err)
		}
		if existing == nil {
			return nil, errors.New("idempotent insert reported a conflict but no job was found")
		}
		return existing, nil
	}

	if err := q.appendEnqueueEvents(job, now); err != nil {
		return nil, err
	}

	return &job, nil
}

// appendEnqueueEvents logs the creation event for a freshly stored job. When
// the job carries a RunAt timestamp, a scheduled event is logged in place of
// the plain enqueued event so the timeline shows the delay.
func (q *Queue) appendEnqueueEvents(job Job, now time.Time) error {
	ev := Event{
		JobID:     job.ID,
		EventType: EventEnqueued,
		Timestamp: now,
	}
	meta, _ := json.Marshal(map[string]string{"kind": job.Kind})
	m := string(meta)
	ev.Metadata = &m
	if job.RunAt != nil {
		ev.EventType = EventScheduled
		m = job.RunAt.UTC().Format(time.RFC3339Nano)
		ev.Metadata = &m
	}
	if err := q.store.AppendEvent(ev); err != nil {
		return fmt.Errorf("log event: %w", err)
	}
	return nil
}

func (q *Queue) Lease(ctx context.Context, kind string, leaseDuration time.Duration) (*Job, error) {
	job, err := q.store.LeaseJob(kind, leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("lease job: %w", err)
	}
	if job == nil {
		return nil, nil
	}

	ev := Event{
		JobID:     job.ID,
		EventType: EventLeased,
		Timestamp: time.Now().UTC(),
	}
	if err := q.store.AppendEvent(ev); err != nil {
		return nil, fmt.Errorf("log event: %w", err)
	}

	return job, nil
}

func (q *Queue) Acknowledge(jobID string) error {
	if err := q.store.CompleteJob(jobID); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	ev := Event{
		JobID:     jobID,
		EventType: EventAcknowledged,
		Timestamp: time.Now().UTC(),
	}
	if err := q.store.AppendEvent(ev); err != nil {
		return fmt.Errorf("log event: %w", err)
	}
	return nil
}

func (q *Queue) Fail(jobID string, errMsg string) error {
	job, err := q.store.GetJob(jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	// MaxAttempts stores the maximum number of attempts, including the first.
	// The current attempt is RetryCount plus one. Retry only when an additional
	// attempt still fits inside the limit.
	shouldRetry := job.RetryCount+1 < job.MaxAttempts
	if err := q.store.FailJob(jobID, shouldRetry); err != nil {
		return fmt.Errorf("fail job: %w", err)
	}

	evType := EventDeadLettered
	meta := fmt.Sprintf("attempt %d/%d exhausted: %s", job.RetryCount+1, job.MaxAttempts, errMsg)
	if shouldRetry {
		evType = EventRetried
		meta = fmt.Sprintf("attempt %d/%d: %s", job.RetryCount+1, job.MaxAttempts, errMsg)
	}
	ev := Event{
		JobID:     jobID,
		EventType: evType,
		Timestamp: time.Now().UTC(),
		Metadata:  &meta,
	}
	if err := q.store.AppendEvent(ev); err != nil {
		return fmt.Errorf("log event: %w", err)
	}
	return nil
}

// Recover returns orphaned leases to the pending state. A lease is orphaned
// when its deadline passed and no worker acknowledged it. Recovered jobs get
// one extra attempt. When no attempt remains, the job enters the dead-letter
// state. Each recovered job logs one event: recovered, or dead_lettered when
// the attempt budget is gone.
func (q *Queue) Recover() (int, error) {
	recovered, err := q.store.RecoverOrphanedLeases()
	if err != nil {
		return 0, fmt.Errorf("recover: %w", err)
	}
	now := time.Now().UTC()
	for _, r := range recovered {
		ev := Event{
			JobID:     r.ID,
			EventType: EventRecovered,
			Timestamp: now,
		}
		meta := fmt.Sprintf("attempt %d/%d", r.RetryCount, r.MaxAttempts)
		if r.State == StateDeadLetter {
			ev.EventType = EventDeadLettered
			meta = fmt.Sprintf("attempt %d/%d exhausted", r.RetryCount, r.MaxAttempts)
		}
		ev.Metadata = &meta
		if err := q.store.AppendEvent(ev); err != nil {
			return len(recovered), fmt.Errorf("log recovery event: %w", err)
		}
	}
	return len(recovered), nil
}

// Requeue returns a dead-lettered job to the pending state with a fresh
// attempt budget. The original job data is kept unless an option overrides it.
// A requeued job can fail again and re-enter the dead-letter queue.
func (q *Queue) Requeue(jobID string, opts ...RequeueOption) (*Job, error) {
	cfg := requeueConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	job, err := q.store.GetJob(jobID)
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if job.State != StateDeadLetter {
		return nil, fmt.Errorf("job %s is %s, not dead-lettered", jobID, job.State)
	}
	maxAttempts := job.MaxAttempts
	if cfg.maxAttempts >= 1 {
		maxAttempts = cfg.maxAttempts
	}

	updated, err := q.store.RequeueJob(jobID, maxAttempts, cfg.payload)
	if err != nil {
		return nil, err
	}

	meta := fmt.Sprintf("attempts reset to 0/%d", maxAttempts)
	ev := Event{
		JobID:     jobID,
		EventType: EventRequeued,
		Timestamp: time.Now().UTC(),
		Metadata:  &meta,
	}
	if err := q.store.AppendEvent(ev); err != nil {
		return nil, fmt.Errorf("log event: %w", err)
	}
	return &updated, nil
}

func (q *Queue) Inspect() (*QueueSnapshot, error) {
	jobs, err := q.store.GetAllJobs()
	if err != nil {
		return nil, fmt.Errorf("get jobs: %w", err)
	}
	events, err := q.store.GetAllEvents()
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	stats, err := q.store.GetQueueStats()
	if err != nil {
		return nil, fmt.Errorf("get stats: %w", err)
	}
	return &QueueSnapshot{Jobs: jobs, Events: events, Stats: stats}, nil
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
