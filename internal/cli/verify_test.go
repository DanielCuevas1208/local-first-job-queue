package cli

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func TestVerifyLeavesLiveDatabaseUnchanged(t *testing.T) {
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
	if _, err := q.Lease(context.Background(), "email", time.Minute); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := q.Acknowledge(job.ID); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	archivePath := filepath.Join(t.TempDir(), "history.jsonl")
	if err := Export([]string{"-db", dbPath, "-out", archivePath}); err != nil {
		t.Fatalf("export: %v", err)
	}

	store, err = queue.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()
	q = queue.NewQueue(store)
	before, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect before: %v", err)
	}
	if err := Verify([]string{"-in", archivePath}); err != nil {
		t.Fatalf("verify: %v", err)
	}
	after, err := q.Inspect()
	if err != nil {
		t.Fatalf("inspect after: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("live snapshot changed after verification:\nbefore=%+v\nafter=%+v", before, after)
	}
}
