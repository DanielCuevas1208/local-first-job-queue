package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/local-first-job-queue/internal/queue"
)

func TestExportWritesVersionedJSONL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	q := queue.NewQueue(store)
	job, err := q.Enqueue("email", `{"to":"user@example.com"}`)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "history.jsonl")
	if err := Export([]string{"-db", dbPath, "-out", outPath}); err != nil {
		t.Fatalf("export: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	var record historyExportRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode archive record: %v", err)
	}
	if got := bytes.Count(data, []byte{'\n'}); got != 1 {
		t.Fatalf("archive has %d lines, want one JSON Lines record", got)
	}
	if record.SchemaVersion != historyExportVersion {
		t.Errorf("schema version = %d, want %d", record.SchemaVersion, historyExportVersion)
	}
	if record.Job.ID != job.ID || record.Job.Payload != job.Payload {
		t.Errorf("job = %+v, want id %s and payload %s", record.Job, job.ID, job.Payload)
	}
	if len(record.Events) != 1 || record.Events[0].EventType != queue.EventEnqueued {
		t.Errorf("events = %+v, want one enqueued event", record.Events)
	}
}

func TestExportFiltersByKind(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	q := queue.NewQueue(store)
	if _, err := q.Enqueue("email", `{}`); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	if _, err := q.Enqueue("report", `{}`); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "email.jsonl")
	if err := Export([]string{"-db", dbPath, "-out", outPath, "-kind", "email"}); err != nil {
		t.Fatalf("export: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	var record historyExportRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode filtered archive: %v", err)
	}
	if record.Job.Kind != "email" {
		t.Fatalf("exported kind = %q, want email", record.Job.Kind)
	}
}
