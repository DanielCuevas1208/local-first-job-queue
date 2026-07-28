package queue

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

func migrate(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			payload TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending',
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			idempotency_key TEXT UNIQUE,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			leased_until TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			metadata TEXT,
			timestamp TEXT NOT NULL,
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_kind_state ON jobs(kind, state)`,
		`CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)`,
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("exec %q: %w", trunc(q, 60), err)
		}
	}
	return nil
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// InsertJob stores a new job. When the job carries an idempotency key that
// already exists, the insert is a no-op and inserted returns false. Callers
// then read the existing job back. This makes Enqueue safe under concurrent
// enqueues with the same key.
func (s *SQLiteStore) InsertJob(j Job) (inserted bool, err error) {
	ik := sql.NullString{}
	if j.IdempotencyKey != nil {
		ik.String = *j.IdempotencyKey
		ik.Valid = true
	}
	lu := sql.NullString{}
	if j.LeasedUntil != nil {
		lu.String = j.LeasedUntil.UTC().Format(time.RFC3339)
		lu.Valid = true
	}
	res, err := s.db.Exec(
		`INSERT INTO jobs (id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(idempotency_key) DO NOTHING`,
		j.ID, j.Kind, j.Payload, string(j.State), j.RetryCount, j.MaxAttempts,
		ik, j.CreatedAt.UTC().Format(time.RFC3339), j.UpdatedAt.UTC().Format(time.RFC3339), lu,
	)
	if err != nil {
		return false, fmt.Errorf("insert job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return n == 1, nil
}

func (s *SQLiteStore) FindJobByIdempotencyKey(key string) (*Job, error) {
	row := s.db.QueryRow(
		`SELECT id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until
		 FROM jobs WHERE idempotency_key = ?`, key)
	job, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *SQLiteStore) GetJob(id string) (Job, error) {
	row := s.db.QueryRow(
		`SELECT id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until
		 FROM jobs WHERE id = ?`, id)
	return scanJob(row)
}

func (s *SQLiteStore) LeaseJob(kind string, leaseDuration time.Duration) (*Job, error) {
	now := time.Now().UTC()
	until := now.Add(leaseDuration)
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRow(
		`SELECT id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until
		 FROM jobs WHERE kind = ? AND state = 'pending'
		 ORDER BY created_at ASC LIMIT 1`, kind)
	job, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query pending: %w", err)
	}

	_, err = tx.Exec(
		`UPDATE jobs SET state = 'leased', leased_until = ?, updated_at = ? WHERE id = ? AND state = 'pending'`,
		until.Format(time.RFC3339), now.Format(time.RFC3339), job.ID)
	if err != nil {
		return nil, fmt.Errorf("update lease: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lease: %w", err)
	}

	job.State = StateLeased
	job.LeasedUntil = &until
	job.UpdatedAt = now
	return &job, nil
}

// LeaseJobByID leases one specific job by its ID. It is used by tools and tests
// that need to simulate a worker leasing and then abandoning a known job. When
// the job is not pending, no lease is taken and the result is nil.
func (s *SQLiteStore) LeaseJobByID(id string, leaseDuration time.Duration) (*Job, error) {
	now := time.Now().UTC()
	until := now.Add(leaseDuration)
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRow(
		`SELECT id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until
		 FROM jobs WHERE id = ?`, id)
	job, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("query job: %w", err)
	}
	if job.State != StatePending {
		return nil, fmt.Errorf("job %s is %s, not pending", id, job.State)
	}

	_, err = tx.Exec(
		`UPDATE jobs SET state = 'leased', leased_until = ?, updated_at = ? WHERE id = ? AND state = 'pending'`,
		until.Format(time.RFC3339), now.Format(time.RFC3339), id)
	if err != nil {
		return nil, fmt.Errorf("update lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lease: %w", err)
	}

	job.State = StateLeased
	job.LeasedUntil = &until
	job.UpdatedAt = now
	return &job, nil
}

func (s *SQLiteStore) CompleteJob(id string) error {
	now := time.Now().UTC()
	result, err := s.db.Exec(
		`UPDATE jobs SET state = 'completed', leased_until = NULL, updated_at = ?
		 WHERE id = ? AND state = 'leased'`,
		now.Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("job %s not found or not leased", id)
	}
	return nil
}

func (s *SQLiteStore) FailJob(id string, retry bool) error {
	now := time.Now().UTC()
	if retry {
		_, err := s.db.Exec(
			`UPDATE jobs SET state = 'pending', retry_count = retry_count + 1, leased_until = NULL, updated_at = ?
			 WHERE id = ? AND state = 'leased'`,
			now.Format(time.RFC3339), id)
		if err != nil {
			return fmt.Errorf("retry job: %w", err)
		}
		return nil
	}
	_, err := s.db.Exec(
		`UPDATE jobs SET state = 'failed', leased_until = NULL, updated_at = ?
		 WHERE id = ? AND state = 'leased'`,
		now.Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}
	return nil
}

// RecoverOrphanedLeases finds jobs whose lease deadline passed and returns
// them to the pending state. Each recovery consumes one attempt. When no
// attempt remains, the job enters the failed state instead of looping.
// The returned slice reports the jobs that were touched, with their updated
// retry counters, so callers can log a recovery event per job.
func (s *SQLiteStore) RecoverOrphanedLeases() ([]Job, error) {
	now := time.Now().UTC()
	rows, err := s.db.Query(
		`SELECT id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until
		 FROM jobs WHERE state = 'leased' AND leased_until < ?`,
		now.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("query orphaned: %w", err)
	}
	defer rows.Close()

	var orphans []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan orphaned job: %w", err)
		}
		orphans = append(orphans, j)
	}
	if len(orphans) == 0 {
		return nil, nil
	}

	for _, j := range orphans {
		nextAttempt := j.RetryCount + 1
		if nextAttempt >= j.MaxAttempts {
			_, err := s.db.Exec(
				`UPDATE jobs SET state = 'failed', leased_until = NULL, updated_at = ?
				 WHERE id = ? AND state = 'leased'`,
				now.Format(time.RFC3339), j.ID)
			if err != nil {
				return nil, fmt.Errorf("fail orphaned job %s: %w", j.ID, err)
			}
			j.RetryCount = nextAttempt
			j.State = StateFailed
			continue
		}
		_, err := s.db.Exec(
			`UPDATE jobs SET state = 'pending', retry_count = ?, leased_until = NULL, updated_at = ?
			 WHERE id = ? AND state = 'leased'`,
			nextAttempt, now.Format(time.RFC3339), j.ID)
		if err != nil {
			return nil, fmt.Errorf("recover job %s: %w", j.ID, err)
		}
		j.RetryCount = nextAttempt
		j.State = StatePending
	}
	return orphans, nil
}

func (s *SQLiteStore) GetPendingJobs() ([]Job, error) {
	return s.queryJobs(`WHERE state = 'pending' ORDER BY created_at ASC`)
}

func (s *SQLiteStore) GetLeasedJobs() ([]Job, error) {
	return s.queryJobs(`WHERE state = 'leased' ORDER BY created_at ASC`)
}

func (s *SQLiteStore) GetAllJobs() ([]Job, error) {
	return s.queryJobs(`ORDER BY created_at DESC`)
}

func (s *SQLiteStore) queryJobs(where string) ([]Job, error) {
	rows, err := s.db.Query(
		`SELECT id, kind, payload, state, retry_count, max_attempts, idempotency_key, created_at, updated_at, leased_until
		 FROM jobs ` + where)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (s *SQLiteStore) GetQueueStats() (map[JobState]int, error) {
	rows, err := s.db.Query(`SELECT state, COUNT(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("query stats: %w", err)
	}
	defer rows.Close()

	stats := map[JobState]int{}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scan stat: %w", err)
		}
		stats[JobState(state)] = count
	}
	return stats, nil
}

func (s *SQLiteStore) AppendEvent(e Event) error {
	md := sql.NullString{}
	if e.Metadata != nil {
		md.String = *e.Metadata
		md.Valid = true
	}
	_, err := s.db.Exec(
		`INSERT INTO events (job_id, event_type, metadata, timestamp)
		 VALUES (?, ?, ?, ?)`,
		e.JobID, string(e.EventType), md, e.Timestamp.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetJobEvents(jobID string) ([]Event, error) {
	rows, err := s.db.Query(
		`SELECT id, job_id, event_type, metadata, timestamp
		 FROM events WHERE job_id = ? ORDER BY id ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *SQLiteStore) GetAllEvents() ([]Event, error) {
	rows, err := s.db.Query(
		`SELECT id, job_id, event_type, metadata, timestamp
		 FROM events ORDER BY id DESC LIMIT 1000`)
	if err != nil {
		return nil, fmt.Errorf("query all events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (Job, error) {
	var j Job
	var stateStr, createdAt, updatedAt string
	var ik, lu sql.NullString
	err := row.Scan(&j.ID, &j.Kind, &j.Payload, &stateStr, &j.RetryCount, &j.MaxAttempts,
		&ik, &createdAt, &updatedAt, &lu)
	if err != nil {
		return Job{}, err
	}
	j.State = JobState(stateStr)
	if j.CreatedAt, err = time.Parse(time.RFC3339, createdAt); err != nil {
		return Job{}, fmt.Errorf("parse created_at: %w", err)
	}
	if j.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt); err != nil {
		return Job{}, fmt.Errorf("parse updated_at: %w", err)
	}
	if ik.Valid {
		j.IdempotencyKey = &ik.String
	}
	if lu.Valid {
		t, err := time.Parse(time.RFC3339, lu.String)
		if err != nil {
			return Job{}, fmt.Errorf("parse leased_until: %w", err)
		}
		j.LeasedUntil = &t
	}
	return j, nil
}

func scanEvents(rows *sql.Rows) ([]Event, error) {
	var events []Event
	for rows.Next() {
		var e Event
		var ts string
		var md sql.NullString
		if err := rows.Scan(&e.ID, &e.JobID, &e.EventType, &md, &ts); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if md.Valid {
			e.Metadata = &md.String
		}
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("parse event timestamp: %w", err)
		}
		e.Timestamp = t
		events = append(events, e)
	}
	return events, nil
}
