// Package archive verifies exported job histories without opening a queue
// database. It treats each JSON Lines record as an isolated history.
package archive

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

// SchemaVersion identifies the JSON Lines history format.
const SchemaVersion = 1

// Record is one versioned JSON Lines history record.
type Record struct {
	SchemaVersion int           `json:"schema_version"`
	Job           queue.Job     `json:"job"`
	Events        []queue.Event `json:"events"`
}

// Issue describes one archive error. Issues make a corrupt archive useful to
// repair because the report keeps the source line and job identifier.
type Issue struct {
	Line    int    `json:"line"`
	JobID   string `json:"job_id,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// VerificationReport summarizes an archive read. Warnings do not make the
// archive invalid; they identify histories whose first retained event is late.
type VerificationReport struct {
	Records      int     `json:"records"`
	ValidRecords int     `json:"valid_records"`
	Issues       []Issue `json:"issues,omitempty"`
	Warnings     []Issue `json:"warnings,omitempty"`
}

// Valid reports whether every record passed the verifier.
func (r VerificationReport) Valid() bool {
	return len(r.Issues) == 0
}

// Verify reads JSON Lines into memory and checks references, event ordering,
// state transitions, and the final state stored in each job snapshot.
//
// The function never opens or writes a SQLite database. A malformed line is
// reported and the next line is still checked. Reader failures return an error.
func Verify(input io.Reader) (VerificationReport, error) {
	var report VerificationReport
	reader := bufio.NewReader(input)
	seenJobs := map[string]int{}
	seenEvents := map[int64]int{}

	for lineNumber := 1; ; lineNumber++ {
		line, err := reader.ReadString('\n')
		if len(strings.TrimSpace(line)) > 0 {
			report.Records++
			var record Record
			if decodeErr := json.Unmarshal([]byte(line), &record); decodeErr != nil {
				report.Issues = append(report.Issues, Issue{
					Line:    lineNumber,
					Code:    "invalid_json",
					Message: decodeErr.Error(),
				})
			} else {
				issues, warnings := validateRecord(record, lineNumber, seenJobs, seenEvents)
				if len(issues) == 0 {
					report.ValidRecords++
				}
				report.Issues = append(report.Issues, issues...)
				report.Warnings = append(report.Warnings, warnings...)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return report, fmt.Errorf("read archive: %w", err)
		}
	}

	return report, nil
}

func validateRecord(record Record, line int, seenJobs map[string]int, seenEvents map[int64]int) ([]Issue, []Issue) {
	var issues []Issue
	var warnings []Issue
	jobID := record.Job.ID
	addIssue := func(code, message string) {
		issues = append(issues, Issue{Line: line, JobID: jobID, Code: code, Message: message})
	}
	addWarning := func(code, message string) {
		warnings = append(warnings, Issue{Line: line, JobID: jobID, Code: code, Message: message})
	}

	if record.SchemaVersion != SchemaVersion {
		addIssue("unsupported_schema", fmt.Sprintf("schema version %d, want %d", record.SchemaVersion, SchemaVersion))
	}
	if jobID == "" {
		addIssue("missing_job_id", "job.id is required")
	} else if previous, exists := seenJobs[jobID]; exists {
		addIssue("duplicate_job", fmt.Sprintf("job also appears on line %d", previous))
	} else {
		seenJobs[jobID] = line
	}
	if record.Job.MaxAttempts < 1 {
		addIssue("invalid_attempt_budget", "job.max_attempts must be at least 1")
	}
	if record.Job.RetryCount < 0 || record.Job.RetryCount > record.Job.MaxAttempts {
		addIssue("invalid_retry_count", fmt.Sprintf("job.retry_count is %d for a budget of %d", record.Job.RetryCount, record.Job.MaxAttempts))
	}
	if !validJobState(record.Job.State) {
		addIssue("invalid_job_state", fmt.Sprintf("unknown job state %q", record.Job.State))
	}
	if !record.Job.CreatedAt.IsZero() && !record.Job.UpdatedAt.IsZero() && record.Job.UpdatedAt.Before(record.Job.CreatedAt) {
		addIssue("invalid_job_time", "job.updated_at is before job.created_at")
	}
	if len(record.Events) == 0 {
		addIssue("missing_history", "events must contain at least one retained event")
		return issues, warnings
	}

	state := replayStateUnknown
	var previousID int64
	var previousTimestamp time.Time
	for index, event := range record.Events {
		if event.ID <= 0 {
			addIssue("invalid_event_id", fmt.Sprintf("event %d has id %d", index+1, event.ID))
		} else if previous, exists := seenEvents[event.ID]; exists {
			addIssue("duplicate_event", fmt.Sprintf("event id %d also appears on line %d", event.ID, previous))
		} else {
			seenEvents[event.ID] = line
		}
		if event.JobID != jobID {
			addIssue("broken_reference", fmt.Sprintf("event %d references job %q, want %q", event.ID, event.JobID, jobID))
		}
		if event.Timestamp.IsZero() {
			addIssue("invalid_event_time", fmt.Sprintf("event %d has no timestamp", event.ID))
		}
		if previousID > 0 && event.ID <= previousID {
			addIssue("invalid_event_order", fmt.Sprintf("event id %d follows %d", event.ID, previousID))
		}
		if !previousTimestamp.IsZero() && !event.Timestamp.IsZero() && event.Timestamp.Before(previousTimestamp) {
			addIssue("invalid_event_order", fmt.Sprintf("event %d timestamp precedes the previous event", event.ID))
		}
		previousID = event.ID
		if !event.Timestamp.IsZero() {
			previousTimestamp = event.Timestamp
		}

		if index == 0 {
			var ok bool
			state, ok = firstEventState(event.EventType)
			if !ok {
				addIssue("invalid_transition", fmt.Sprintf("event %q cannot start a history", event.EventType))
				continue
			}
			if event.EventType != queue.EventEnqueued && event.EventType != queue.EventScheduled {
				addWarning("history_truncated", fmt.Sprintf("history starts with %q; earlier events may have been removed", event.EventType))
			}
			continue
		}

		next, ok := transition(state, event.EventType)
		if !ok {
			addIssue("invalid_transition", fmt.Sprintf("event %q cannot follow state %q", event.EventType, state))
			continue
		}
		state = next
	}

	if state != replayStateUnknown && string(state) != string(record.Job.State) {
		addIssue("state_mismatch", fmt.Sprintf("events end in %q, job.state is %q", state, record.Job.State))
	}
	return issues, warnings
}

type replayState string

const (
	replayStateUnknown    replayState = "unknown"
	replayStatePending    replayState = "pending"
	replayStateLeased     replayState = "leased"
	replayStateCompleted  replayState = "completed"
	replayStateFailed     replayState = "failed"
	replayStateDeadLetter replayState = "dead_letter"
)

func firstEventState(eventType queue.EventType) (replayState, bool) {
	switch eventType {
	case queue.EventEnqueued, queue.EventScheduled:
		return replayStatePending, true
	case queue.EventLeased, queue.EventLeaseRenewed:
		return replayStateLeased, true
	case queue.EventAcknowledged:
		return replayStateCompleted, true
	case queue.EventFailed:
		return replayStateFailed, true
	case queue.EventRetried, queue.EventRecovered, queue.EventRequeued:
		return replayStatePending, true
	case queue.EventDeadLettered:
		return replayStateDeadLetter, true
	default:
		return replayStateUnknown, false
	}
}

func transition(state replayState, eventType queue.EventType) (replayState, bool) {
	switch eventType {
	case queue.EventLeased:
		return replayStateLeased, state == replayStatePending
	case queue.EventLeaseRenewed:
		return replayStateLeased, state == replayStateLeased
	case queue.EventAcknowledged:
		return replayStateCompleted, state == replayStateLeased
	case queue.EventFailed:
		return replayStateFailed, state == replayStateLeased
	case queue.EventRetried, queue.EventRecovered:
		return replayStatePending, state == replayStateLeased
	case queue.EventDeadLettered:
		return replayStateDeadLetter, state == replayStateLeased
	case queue.EventRequeued:
		return replayStatePending, state == replayStateDeadLetter
	default:
		return replayStateUnknown, false
	}
}

func validJobState(state queue.JobState) bool {
	switch state {
	case queue.StatePending, queue.StateLeased, queue.StateCompleted, queue.StateFailed, queue.StateDeadLetter:
		return true
	default:
		return false
	}
}
