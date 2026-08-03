package queue

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteTimeFormat = time.RFC3339Nano

// DefaultAgingInterval is the recommended priority aging interval. A job gains
// one priority point per interval it has waited, which prevents a constant
// high-priority stream from starving lower-priority work. The work command
// uses this value unless the operator overrides it.
const DefaultAgingInterval = 30 * time.Second

type StoreOption func(*storeConfig)

type storeConfig struct {
	agingInterval time.Duration
	agingSet      bool
}

// WithAgingInterval sets the priority aging interval. A pending job gains one
// priority point per interval it has waited, so a lower-priority job eventually
// overtakes a constant stream of higher-priority work. A zero interval keeps
// the plain priority ordering. When the option is absent, aging is disabled.
func WithAgingInterval(d time.Duration) StoreOption {
	return func(c *storeConfig) {
		c.agingInterval = d
		c.agingSet = true
	}
}

type SQLiteStore struct {
	db            *sql.DB
	agingInterval time.Duration
}

func NewSQLiteStore(dbPath string, opts ...StoreOption) (*SQLiteStore, error) {
	cfg := storeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &SQLiteStore{db: db, agingInterval: cfg.agingInterval}, nil
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
			priority INTEGER NOT NULL DEFAULT 0,
			idempotency_key TEXT UNIQUE,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			leased_until TEXT,
			run_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			metadata TEXT,
			timestamp TEXT NOT NULL,
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("exec %q: %w", trunc(q, 60), err)
		}
	}
	if err := ensureColumn(db, "jobs", "priority", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "jobs", "run_at", "TEXT"); err != nil {
		return err
	}
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_kind_state ON jobs(kind, state)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_kind_ready ON jobs(kind, state, run_at)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_kind_priority ON jobs(kind, state, priority DESC, run_at, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)`,
	}
	for _, q := range indexes {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("exec %q: %w", trunc(q, 60), err)
		}
	}
	return nil
}

// ensureColumn adds a missing column to an existing table. Databases created
// before a schema change still gain the column on the next open. The caller is
// responsible for the column type, nullability, and default value.
func ensureColumn(db *sql.DB, table, column, colType string) error {
	rows, err := db.Query(`PRAGMA table_info("` + table + `")`)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return fmt.Errorf("scan %s info: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, colType)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
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
		lu.String = j.LeasedUntil.UTC().Format(sqliteTimeFormat)
		lu.Valid = true
	}
	ra := sql.NullString{}
	if j.RunAt != nil {
		ra.String = j.RunAt.UTC().Format(sqliteTimeFormat)
		ra.Valid = true
	}
	res, err := s.db.Exec(
		`INSERT INTO jobs (id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(idempotency_key) DO NOTHING`,
		j.ID, j.Kind, j.Payload, string(j.State), j.RetryCount, j.MaxAttempts, j.Priority,
		ik, j.CreatedAt.UTC().Format(sqliteTimeFormat), j.UpdatedAt.UTC().Format(sqliteTimeFormat), lu, ra,
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
		`SELECT id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at
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
		`SELECT id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at
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

	orderBy, orderArgs := s.pendingOrderBy(now)
	query := `SELECT id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at
		 FROM jobs WHERE kind = ? AND state = 'pending'
		   AND (run_at IS NULL OR run_at <= ?) ORDER BY ` + orderBy + ` LIMIT 1`
	args := append([]any{kind, now.Format(sqliteTimeFormat)}, orderArgs...)
	row := tx.QueryRow(query, args...)
	job, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query pending: %w", err)
	}

	_, err = tx.Exec(
		`UPDATE jobs SET state = 'leased', leased_until = ?, updated_at = ? WHERE id = ? AND state = 'pending'`,
		until.Format(sqliteTimeFormat), now.Format(sqliteTimeFormat), job.ID)
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
		`SELECT id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at
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
		until.Format(sqliteTimeFormat), now.Format(sqliteTimeFormat), id)
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
		now.Format(sqliteTimeFormat), id)
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
			now.Format(sqliteTimeFormat), id)
		if err != nil {
			return fmt.Errorf("retry job: %w", err)
		}
		return nil
	}
	_, err := s.db.Exec(
		`UPDATE jobs SET state = 'dead_letter', retry_count = retry_count + 1, leased_until = NULL, updated_at = ?
		 WHERE id = ? AND state = 'leased'`,
		now.Format(sqliteTimeFormat), id)
	if err != nil {
		return fmt.Errorf("dead-letter job: %w", err)
	}
	return nil
}

// RequeueJob returns a dead-lettered job to the pending state with a fresh
// attempt budget. The caller may supply a new payload and a new attempt limit.
// When the job is not dead-lettered, the update matches no row and an error is
// returned.
func (s *SQLiteStore) RequeueJob(id string, maxAttempts int, payload *string) (Job, error) {
	now := time.Now().UTC()
	var (
		res sql.Result
		err error
	)
	if payload != nil {
		res, err = s.db.Exec(
			`UPDATE jobs SET state = 'pending', retry_count = 0, max_attempts = ?,
			   payload = ?, leased_until = NULL, updated_at = ?
			 WHERE id = ? AND state = 'dead_letter'`,
			maxAttempts, *payload, now.Format(sqliteTimeFormat), id)
	} else {
		res, err = s.db.Exec(
			`UPDATE jobs SET state = 'pending', retry_count = 0, max_attempts = ?,
			   leased_until = NULL, updated_at = ?
			 WHERE id = ? AND state = 'dead_letter'`,
			maxAttempts, now.Format(sqliteTimeFormat), id)
	}
	if err != nil {
		return Job{}, fmt.Errorf("requeue job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return Job{}, fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return Job{}, fmt.Errorf("job %s is not dead-lettered", id)
	}
	return s.GetJob(id)
}

// RecoverOrphanedLeases finds jobs whose lease deadline passed and returns
// them to the pending state. Each recovery consumes one attempt. When no
// attempt remains, the job enters the dead-letter state instead of looping.
// The returned slice reports the jobs that were touched, with their updated
// retry counters, so callers can log a recovery event per job.
func (s *SQLiteStore) RecoverOrphanedLeases() ([]Job, error) {
	now := time.Now().UTC()
	rows, err := s.db.Query(
		`SELECT id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at
		 FROM jobs WHERE state = 'leased' AND leased_until < ?`,
		now.Format(sqliteTimeFormat))
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

	for i := range orphans {
		j := &orphans[i]
		nextAttempt := j.RetryCount + 1
		if nextAttempt >= j.MaxAttempts {
			_, err := s.db.Exec(
				`UPDATE jobs SET state = 'dead_letter', retry_count = ?, leased_until = NULL, updated_at = ?
				 WHERE id = ? AND state = 'leased'`,
				nextAttempt, now.Format(sqliteTimeFormat), j.ID)
			if err != nil {
				return nil, fmt.Errorf("dead-letter orphaned job %s: %w", j.ID, err)
			}
			j.RetryCount = nextAttempt
			j.State = StateDeadLetter
			continue
		}
		_, err := s.db.Exec(
			`UPDATE jobs SET state = 'pending', retry_count = ?, leased_until = NULL, updated_at = ?
			 WHERE id = ? AND state = 'leased'`,
			nextAttempt, now.Format(sqliteTimeFormat), j.ID)
		if err != nil {
			return nil, fmt.Errorf("recover job %s: %w", j.ID, err)
		}
		j.RetryCount = nextAttempt
		j.State = StatePending
	}
	return orphans, nil
}

// pendingOrderBy builds the ORDER BY clause for ready jobs. When aging is
// enabled, a job gains one priority point for each aging interval it has
// waited since it became ready. The boost is added to the stored priority, so
// an older low-priority job can overtake a fresher high-priority job. The
// returned arguments bind the "now" timestamp and the aging interval, and are
// appended to the caller's query arguments. Aging is measured from the earlier
// of run_at and created_at, so a scheduled job only ages once it is ready.
func (s *SQLiteStore) pendingOrderBy(now time.Time) (string, []any) {
	if s.agingInterval <= 0 {
		return `priority DESC, COALESCE(run_at, created_at) ASC, created_at ASC, id ASC`, nil
	}
	agingSec := s.agingInterval.Seconds()
	orderBy := `(priority + CAST((julianday(?) - julianday(COALESCE(run_at, created_at))) * 86400.0 / ? AS INTEGER)) DESC,
		priority DESC, COALESCE(run_at, created_at) ASC, created_at ASC, id ASC`
	args := []any{now.Format(sqliteTimeFormat), agingSec}
	return orderBy, args
}

func (s *SQLiteStore) GetPendingJobs() ([]Job, error) {
	orderBy, orderArgs := s.pendingOrderBy(time.Now().UTC())
	where := "WHERE state = 'pending' ORDER BY " + orderBy
	return s.queryJobs(where, orderArgs...)
}

func (s *SQLiteStore) GetLeasedJobs() ([]Job, error) {
	return s.queryJobs(`WHERE state = 'leased' ORDER BY created_at ASC`)
}

func (s *SQLiteStore) GetAllJobs() ([]Job, error) {
	return s.queryJobs(`ORDER BY created_at DESC`)
}

func (s *SQLiteStore) queryJobs(where string, args ...any) ([]Job, error) {
	rows, err := s.db.Query(
		`SELECT id, kind, payload, state, retry_count, max_attempts, priority, idempotency_key, created_at, updated_at, leased_until, run_at
		 FROM jobs `+where, args...)
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

// GetStateKindCounts returns the number of jobs for each (kind, state) pair.
// The result is stable, so exporters can render it in a fixed order.
func (s *SQLiteStore) GetStateKindCounts() ([]KindStateCount, error) {
	rows, err := s.db.Query(`SELECT kind, state, COUNT(*) FROM jobs GROUP BY kind, state ORDER BY kind, state`)
	if err != nil {
		return nil, fmt.Errorf("query kind counts: %w", err)
	}
	defer rows.Close()

	var counts []KindStateCount
	for rows.Next() {
		var c KindStateCount
		var state string
		if err := rows.Scan(&c.Kind, &state, &c.Count); err != nil {
			return nil, fmt.Errorf("scan kind count: %w", err)
		}
		c.State = JobState(state)
		counts = append(counts, c)
	}
	return counts, nil
}

// GetEventTypeCounts returns the number of events for each event type. The
// event log is append-only, so each count grows and never shrinks.
func (s *SQLiteStore) GetEventTypeCounts() ([]EventTypeCount, error) {
	rows, err := s.db.Query(`SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY event_type`)
	if err != nil {
		return nil, fmt.Errorf("query event counts: %w", err)
	}
	defer rows.Close()

	var counts []EventTypeCount
	for rows.Next() {
		var c EventTypeCount
		var et string
		if err := rows.Scan(&et, &c.Count); err != nil {
			return nil, fmt.Errorf("scan event count: %w", err)
		}
		c.EventType = EventType(et)
		counts = append(counts, c)
	}
	return counts, nil
}

// GetOldestPendingReadyTime returns the ready time of the oldest pending job.
// The ready time is the earlier of run_at and created_at, matching the lease
// ordering. ok is false when no job is pending.
func (s *SQLiteStore) GetOldestPendingReadyTime() (ready time.Time, ok bool, err error) {
	var raw string
	err = s.db.QueryRow(
		`SELECT COALESCE(run_at, created_at) FROM jobs
		 WHERE state = 'pending' ORDER BY COALESCE(run_at, created_at) ASC LIMIT 1`).Scan(&raw)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("query oldest pending: %w", err)
	}
	t, err := time.Parse(sqliteTimeFormat, raw)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse oldest pending: %w", err)
	}
	return t, true, nil
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
		e.JobID, string(e.EventType), md, e.Timestamp.UTC().Format(sqliteTimeFormat))
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
	var ik, lu, ra sql.NullString
	err := row.Scan(&j.ID, &j.Kind, &j.Payload, &stateStr, &j.RetryCount, &j.MaxAttempts,
		&j.Priority, &ik, &createdAt, &updatedAt, &lu, &ra)
	if err != nil {
		return Job{}, err
	}
	j.State = JobState(stateStr)
	if j.CreatedAt, err = time.Parse(sqliteTimeFormat, createdAt); err != nil {
		return Job{}, fmt.Errorf("parse created_at: %w", err)
	}
	if j.UpdatedAt, err = time.Parse(sqliteTimeFormat, updatedAt); err != nil {
		return Job{}, fmt.Errorf("parse updated_at: %w", err)
	}
	if ik.Valid {
		j.IdempotencyKey = &ik.String
	}
	if lu.Valid {
		t, err := time.Parse(sqliteTimeFormat, lu.String)
		if err != nil {
			return Job{}, fmt.Errorf("parse leased_until: %w", err)
		}
		j.LeasedUntil = &t
	}
	if ra.Valid {
		t, err := time.Parse(sqliteTimeFormat, ra.String)
		if err != nil {
			return Job{}, fmt.Errorf("parse run_at: %w", err)
		}
		j.RunAt = &t
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
		t, err := time.Parse(sqliteTimeFormat, ts)
		if err != nil {
			return nil, fmt.Errorf("parse event timestamp: %w", err)
		}
		e.Timestamp = t
		events = append(events, e)
	}
	return events, nil
}
