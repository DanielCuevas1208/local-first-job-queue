package cli

import (
	"flag"
	"fmt"

	"github.com/local-first-job-queue/internal/fixture"
	"github.com/local-first-job-queue/internal/queue"
)

// Seed loads the bundled sample data into the database. Each job uses an
// idempotency key, so running Seed more than once does not duplicate jobs.
func Seed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	dbPath := fs.String("db", "queue.db", "database path")
	fs.Parse(args)

	store, err := queue.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	q := queue.NewQueue(store)
	jobs, err := fixture.LoadSampleData(q)
	if err != nil {
		return fmt.Errorf("load sample data: %w", err)
	}

	fmt.Printf("seeded %d jobs across %d kinds\n", len(jobs), len(fixture.Workloads))
	fmt.Println("run 'jobqueue inspect -db " + *dbPath + "' to see them.")
	return nil
}
