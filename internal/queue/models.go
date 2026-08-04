package queue

import "time"

type JobState string

const (
	StatePending   JobState = "pending"
	StateLeased    JobState = "leased"
	StateCompleted JobState = "completed"
	StateFailed    JobState = "failed"
	// StateDeadLetter is the terminal state for jobs that exhausted their
	// attempt budget. A dead-lettered job stays inspectable and an operator can
	// requeue it with the Requeue method.
	StateDeadLetter JobState = "dead_letter"
)

type Job struct {
	ID             string     `json:"id"`
	Kind           string     `json:"kind"`
	Payload        string     `json:"payload"`
	State          JobState   `json:"state"`
	RetryCount     int        `json:"retry_count"`
	MaxAttempts    int        `json:"max_attempts"`
	Priority       int        `json:"priority"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	LeasedUntil    *time.Time `json:"leased_until,omitempty"`
	// RunAt is the earliest time the job may be leased. When nil, the job is
	// ready as soon as it is pending. The field is stored as RFC3339 UTC.
	RunAt *time.Time `json:"run_at,omitempty"`
}

type EventType string

const (
	EventEnqueued     EventType = "enqueued"
	EventScheduled    EventType = "scheduled"
	EventLeased       EventType = "leased"
	EventAcknowledged EventType = "acknowledged"
	EventFailed       EventType = "failed"
	EventRetried      EventType = "retried"
	EventRecovered    EventType = "recovered"
	EventDeadLettered EventType = "dead_lettered"
	EventRequeued     EventType = "requeued"
)

type Event struct {
	ID        int64     `json:"id"`
	JobID     string    `json:"job_id"`
	EventType EventType `json:"event_type"`
	Metadata  *string   `json:"metadata,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type QueueSnapshot struct {
	Jobs   []Job            `json:"jobs"`
	Events []Event          `json:"events"`
	Stats  map[JobState]int `json:"stats"`
}

// KindStateCount reports how many jobs share one kind and state. Metrics and
// inspection tools group jobs by these two dimensions.
type KindStateCount struct {
	Kind  string   `json:"kind"`
	State JobState `json:"state"`
	Count int      `json:"count"`
}

// EventTypeCount reports how many events share one event type. The count grows
// as jobs progress and can drop when a retention run trims the log.
type EventTypeCount struct {
	EventType EventType `json:"event_type"`
	Count     int       `json:"count"`
}

// RetentionSource identifies who started a retention run. The prune command
// records manual runs; the worker records the automatic runs it performs on a
// schedule. The source lets operators and exporters see where retention
// activity came from.
type RetentionSource string

const (
	// RetentionSourceManual labels runs from the prune command or a direct
	// Prune call.
	RetentionSourceManual RetentionSource = "manual"
	// RetentionSourceAuto labels runs the worker performs on its own schedule.
	RetentionSourceAuto RetentionSource = "auto"
)

// RetentionRun records one retention pass: when it ran, who ran it, the policy
// it applied, and how many rows it removed. The store keeps the log append-only
// so operators can review what retention has done.
type RetentionRun struct {
	ID              int64           `json:"id"`
	StartedAt       time.Time       `json:"started_at"`
	Source          RetentionSource `json:"source"`
	MaxJobAge       string          `json:"max_job_age"`
	MaxEventsPerJob int             `json:"max_events_per_job"`
	JobsRemoved     int             `json:"jobs_removed"`
	EventsRemoved   int             `json:"events_removed"`
}

// RetentionSourceCount reports how many retention runs one source recorded and
// what those runs removed. Metrics exporters group counters by source.
type RetentionSourceCount struct {
	Source        RetentionSource `json:"source"`
	Runs          int             `json:"runs"`
	JobsRemoved   int             `json:"jobs_removed"`
	EventsRemoved int             `json:"events_removed"`
}
