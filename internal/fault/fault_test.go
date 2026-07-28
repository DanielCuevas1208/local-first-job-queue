package fault

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/local-first-job-queue/internal/queue"
)

func noop(_ context.Context, _ queue.Job) error { return nil }

func jobWith(payload string, retryCount int) queue.Job {
	return queue.Job{ID: "j1", Kind: "test", Payload: payload, RetryCount: retryCount, MaxAttempts: 3}
}

func TestInjectorNoFault(t *testing.T) {
	called := false
	in := New(func(context.Context, queue.Job) error {
		called = true
		return nil
	}, FromPayload)

	err := in.Handle(context.Background(), jobWith(`{"msg":"ok"}`, 0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("inner handler was not called")
	}
}

func TestInjectorErrorUntilAttempt(t *testing.T) {
	in := New(noop, Constant(Policy{Mode: ModeError, FailUntilAttempt: 2, Message: "boom"}))

	if err := in.Handle(context.Background(), jobWith(`{}`, 0)); err == nil {
		t.Error("expected error on attempt 1")
	}
	if err := in.Handle(context.Background(), jobWith(`{}`, 1)); err == nil {
		t.Error("expected error on attempt 2 (retry)")
	}
	if err := in.Handle(context.Background(), jobWith(`{}`, 2)); err != nil {
		t.Errorf("expected success on attempt 3, got %v", err)
	}
	if err := in.Handle(context.Background(), jobWith(`{}`, 3)); err != nil {
		t.Errorf("expected success after attempts exhausted, got %v", err)
	}
}

func TestInjectorPanicBecomesError(t *testing.T) {
	in := New(noop, Constant(Policy{Mode: ModePanic, FailUntilAttempt: 1, Message: "explode"}))

	err := in.Handle(context.Background(), jobWith(`{}`, 0))
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	if !strings.Contains(err.Error(), "explode") {
		t.Errorf("expected panic message in error, got %q", err.Error())
	}

	// Attempt 2 must run the inner handler normally.
	if err := in.Handle(context.Background(), jobWith(`{}`, 1)); err != nil {
		t.Errorf("expected no error after panic window, got %v", err)
	}
}

func TestInjectorSlowRespectsContext(t *testing.T) {
	in := New(noop, Constant(Policy{Mode: ModeSlow}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := in.Handle(ctx, jobWith(`{}`, 0))
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed < 15*time.Millisecond {
		t.Errorf("expected to block until deadline, elapsed=%v", elapsed)
	}
}

func TestInjectorDelayIsCancellable(t *testing.T) {
	in := New(noop, Constant(Policy{Mode: ModeNone, Delay: 5 * time.Second}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if err := in.Handle(ctx, jobWith(`{}`, 0)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected deadline exceeded during delay, got %v", err)
	}
}

func TestFromPayloadParsesSpec(t *testing.T) {
	p := FromPayload(jobWith(`{"fault":{"mode":"error","fail_until_attempt":2,"message":"disk full"}}`, 0))
	if p.Mode != ModeError {
		t.Errorf("expected mode error, got %s", p.Mode)
	}
	if p.FailUntilAttempt != 2 {
		t.Errorf("expected fail_until_attempt 2, got %d", p.FailUntilAttempt)
	}
	if p.Message != "disk full" {
		t.Errorf("expected message, got %q", p.Message)
	}
}

func TestFromPayloadIgnoresBadJSON(t *testing.T) {
	p := FromPayload(jobWith(`not json`, 0))
	if p.Mode != ModeNone {
		t.Errorf("expected none for bad payload, got %s", p.Mode)
	}
}

func TestFromPayloadEmptyFault(t *testing.T) {
	p := FromPayload(jobWith(`{"msg":"ok"}`, 0))
	if p.Mode != ModeNone {
		t.Errorf("expected none when fault missing, got %s", p.Mode)
	}
}
