package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/local-first-job-queue/internal/queue"
)

const historyExportVersion = 1

type historyExportRecord struct {
	SchemaVersion int           `json:"schema_version"`
	Job           queue.Job     `json:"job"`
	Events        []queue.Event `json:"events"`
}

// Export writes current job snapshots and their retained event timelines as
// versioned JSON Lines. The archive stays pipe-friendly when -out is omitted.
func Export(args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	outPath := fs.String("out", "-", "JSONL output path; - writes to stdout")
	kind := fs.String("kind", "", "export only this job kind")
	state := fs.String("state", "", "export only this job state")
	fs.Parse(args)

	if fs.NArg() > 0 {
		return fmt.Errorf("usage: export [-db path] [-out path] [-kind type] [-state state]")
	}

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	histories, err := store.GetJobHistories(queue.HistoryFilter{
		Kind:  *kind,
		State: queue.JobState(*state),
	})
	if err != nil {
		return fmt.Errorf("read history: %w", err)
	}

	if *outPath == "-" {
		if _, err := writeHistoryExport(os.Stdout, histories); err != nil {
			return fmt.Errorf("write history: %w", err)
		}
		return nil
	}

	file, err := os.OpenFile(*outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	count, writeErr := writeHistoryExport(file, histories)
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write history: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close output: %w", closeErr)
	}
	fmt.Fprintf(os.Stderr, "exported %d job histories to %s\n", count, *outPath)
	return nil
}

func writeHistoryExport(w io.Writer, histories []queue.JobHistory) (int, error) {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, history := range histories {
		record := historyExportRecord{
			SchemaVersion: historyExportVersion,
			Job:           history.Job,
			Events:        history.Events,
		}
		if err := enc.Encode(record); err != nil {
			return 0, err
		}
	}
	return len(histories), nil
}
