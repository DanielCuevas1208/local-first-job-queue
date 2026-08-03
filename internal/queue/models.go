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

// EventTypeCount reports how many events share one event type. The count is
// monotonic while the event log keeps every row.
type EventTypeCount struct {
	EventType EventType `json:"event_type"`
	Count     int       `json:"count"`
}
