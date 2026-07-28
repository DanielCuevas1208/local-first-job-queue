// Package fault provides a deterministic fault-injection harness for worker
// handlers. An Injector wraps an inner handler and applies a Policy to each
// job. Policies decide what failure to synthesize based on the job payload
// and the current attempt number, so the outcome of a run is reproducible.
//
// The harness is intended for demos and tests. It lets a single worker binary
// exercise the queue retry, lease-expiry, and crash-recovery paths without
// reaching for an external chaos tool.
package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/local-first-job-queue/internal/queue"
	"github.com/local-first-job-queue/internal/worker"
)

// Mode selects the kind of fault that a Policy injects.
type Mode string

const (
	// ModeNone runs the inner handler without any injected failure.
	ModeNone Mode = "none"
	// ModeError returns an error from the handler.
	ModeError Mode = "error"
	// ModePanic panics inside the handler. The worker recovers panics so the
	// job is recorded as failed and retried normally.
	ModePanic Mode = "panic"
	// ModeSlow sleeps long enough to push the job past its lease deadline.
	// The job then appears orphaned and is recovered by the next worker run.
	ModeSlow Mode = "slow"
	// ModeStall blocks until the supplied context is cancelled. Use it to
	// simulate a worker that hangs and is then killed.
	ModeStall Mode = "stall"
)

// Policy describes the fault to inject for one job. FailUntilAttempt is the
// highest attempt number (one-based) that should fail. Attempt zero is the
// first run, attempt one is the first retry, and so on.
type Policy struct {
	Mode             Mode          `json:"mode"`
	FailUntilAttempt int           `json:"fail_until_attempt"`
	Delay            time.Duration `json:"delay"`
	Message          string        `json:"message"`
}

// PolicyFunc returns the Policy for a job. Callers can read the job payload to
// build a per-job policy.
type PolicyFunc func(job queue.Job) Policy

// FromPayload reads a fault spec from the job payload. The payload is expected
// to be a JSON object. When the object contains a "fault" field, that field is
// decoded into a Policy. Otherwise the policy is ModeNone.
//
//	{"fault":{"mode":"error","fail_until_attempt":2,"message":"disk full"}}
//
// A delay duration field is accepted as a number of milliseconds.
func FromPayload(job queue.Job) Policy {
	var spec struct {
		Fault struct {
			Mode             string `json:"mode"`
			FailUntilAttempt int    `json:"fail_until_attempt"`
			DelayMs          int    `json:"delay_ms"`
			Message          string `json:"message"`
		} `json:"fault"`
	}
	if err := json.Unmarshal([]byte(job.Payload), &spec); err != nil {
		return Policy{Mode: ModeNone}
	}
	if spec.Fault.Mode == "" {
		return Policy{Mode: ModeNone}
	}
	return Policy{
		Mode:             Mode(spec.Fault.Mode),
		FailUntilAttempt: spec.Fault.FailUntilAttempt,
		Delay:            time.Duration(spec.Fault.DelayMs) * time.Millisecond,
		Message:          spec.Fault.Message,
	}
}

// Constant returns a PolicyFunc that always reports the same Policy.
func Constant(p Policy) PolicyFunc {
	return func(queue.Job) Policy { return p }
}

// Injector wraps a handler and applies a Policy to each job.
type Injector struct {
	inner  worker.JobHandler
	policy PolicyFunc
}

// New returns an Injector that uses policy to decide the fault per job.
func New(inner worker.JobHandler, policy PolicyFunc) *Injector {
	if policy == nil {
		policy = FromPayload
	}
	return &Injector{inner: inner, policy: policy}
}

// Handle runs the inner handler under the policy. The attempt number is
// derived from the job RetryCount so the result does not depend on mutable
// state inside the Injector. A panic from the inner handler or from a
// ModePanic policy is recovered and returned as an error.
func (in *Injector) Handle(ctx context.Context, job queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered panic: %v", r)
		}
	}()

	p := in.policy(job)
	attempt := job.RetryCount + 1

	if p.Delay > 0 {
		if err := sleep(ctx, p.Delay); err != nil {
			return err
		}
	}

	switch p.Mode {
	case ModeStall:
		return stall(ctx)
	case ModeSlow:
		// Block until the caller cancels us when the lease expires. The job
		// then appears orphaned and is recovered by the next worker run.
		return stall(ctx)
	case ModePanic:
		if attempt <= p.FailUntilAttempt {
			panic(fmt.Sprintf("injected panic on attempt %d: %s", attempt, p.Message))
		}
	case ModeError:
		if attempt <= p.FailUntilAttempt {
			msg := p.Message
			if msg == "" {
				msg = fmt.Sprintf("injected error on attempt %d", attempt)
			}
			return errors.New(msg)
		}
	case ModeNone:
		// Run the inner handler.
	}

	return in.inner(ctx, job)
}

var _ worker.JobHandler = (&Injector{}).Handle

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func stall(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
