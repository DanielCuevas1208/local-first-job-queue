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

// StateOrder lists the job states in canonical display order. Exporters,
// the web UI, and the inspect command share this order so every consumer
// renders state consistently. The failed state appears for legacy databases
// that predate the dead-letter queue.
var StateOrder = []JobState{
	StatePending,
	StateLeased,
	StateCompleted,
	StateDeadLetter,
	StateFailed,
}

// EventTypeOrder lists the event types in canonical display order. It is the
// shared order for event counts, keeping exports stable across consumers.
var EventTypeOrder = []EventType{
	EventEnqueued,
	EventScheduled,
	EventLeased,
	EventAcknowledged,
	EventFailed,
	EventRetried,
	EventRecovered,
	EventDeadLettered,
	EventRequeued,
}

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

// JobFilter narrows a ListJobs call. Empty Kind and State fields match any
// value. A zero Limit returns up to DefaultListLimit jobs.
type JobFilter struct {
	Kind  string   `json:"kind,omitempty"`
	State JobState `json:"state,omitempty"`
	Limit int      `json:"limit,omitempty"`
}

// EventTypeCount reports how many events share one event type. The count is
// monotonic while the event log keeps every row.
type EventTypeCount struct {
	EventType EventType `json:"event_type"`
	Count     int       `json:"count"`
}
