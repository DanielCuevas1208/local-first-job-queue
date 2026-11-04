package archive

import (
	"strings"
	"testing"
)

func TestVerifyAcceptsCompleteHistory(t *testing.T) {
	report, err := Verify(strings.NewReader(`{"schema_version":1,"job":{"id":"job-1","kind":"email","state":"completed","max_attempts":3,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:03Z"},"events":[{"id":1,"job_id":"job-1","event_type":"enqueued","timestamp":"2026-01-01T00:00:00Z"},{"id":2,"job_id":"job-1","event_type":"leased","timestamp":"2026-01-01T00:00:01Z"},{"id":3,"job_id":"job-1","event_type":"lease_renewed","timestamp":"2026-01-01T00:00:02Z"},{"id":4,"job_id":"job-1","event_type":"acknowledged","timestamp":"2026-01-01T00:00:03Z"}]}`))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !report.Valid() || report.ValidRecords != 1 || len(report.Warnings) != 0 {
		t.Fatalf("report = %+v, want one valid record", report)
	}
}

func TestVerifyReportsBrokenReference(t *testing.T) {
	input := `{"schema_version":1,"job":{"id":"job-1","state":"pending","max_attempts":3},"events":[{"id":1,"job_id":"other","event_type":"enqueued","timestamp":"2026-01-01T00:00:00Z"}]}`

	report, err := Verify(strings.NewReader(input))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Valid() || !hasIssue(report.Issues, "broken_reference") {
		t.Fatalf("report = %+v, want broken reference", report)
	}
}

func TestVerifyReportsInvalidTransition(t *testing.T) {
	input := `{"schema_version":1,"job":{"id":"job-1","state":"completed","max_attempts":3},"events":[{"id":1,"job_id":"job-1","event_type":"enqueued","timestamp":"2026-01-01T00:00:00Z"},{"id":2,"job_id":"job-1","event_type":"acknowledged","timestamp":"2026-01-01T00:00:01Z"}]}`

	report, err := Verify(strings.NewReader(input))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Valid() || !hasIssue(report.Issues, "invalid_transition") {
		t.Fatalf("report = %+v, want invalid transition", report)
	}
}

func TestVerifyContinuesAfterCorruptLine(t *testing.T) {
	input := "not-json\n" + `{"schema_version":1,"job":{"id":"job-1","state":"pending","max_attempts":3},"events":[{"id":1,"job_id":"job-1","event_type":"enqueued","timestamp":"2026-01-01T00:00:00Z"}]}` + "\n"

	report, err := Verify(strings.NewReader(input))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Records != 2 || report.ValidRecords != 1 || !hasIssue(report.Issues, "invalid_json") {
		t.Fatalf("report = %+v, want one valid record and one JSON issue", report)
	}
}

func TestVerifyWarnsForRetainedSuffix(t *testing.T) {
	input := `{"schema_version":1,"job":{"id":"job-1","state":"completed","max_attempts":3},"events":[{"id":2,"job_id":"job-1","event_type":"leased","timestamp":"2026-01-01T00:00:01Z"},{"id":3,"job_id":"job-1","event_type":"acknowledged","timestamp":"2026-01-01T00:00:02Z"}]}`

	report, err := Verify(strings.NewReader(input))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !report.Valid() || !hasIssue(report.Warnings, "history_truncated") {
		t.Fatalf("report = %+v, want a retained-history warning", report)
	}
}

func hasIssue(issues []Issue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}
