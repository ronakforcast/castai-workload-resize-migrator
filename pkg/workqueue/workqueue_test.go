package workqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/detector"
)

func newItem(name string) *detector.PodPendingInfo {
	return &detector.PodPendingInfo{
		Namespace: "default",
		PodName:   name,
	}
}

// waitFor spins (up to d) until cond returns true. Returns true on success.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestStartProcessesAllItems(t *testing.T) {
	q := New(16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64
	var seen []string
	var mu sync.Mutex
	q.Start(ctx, 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		mu.Lock()
		seen = append(seen, p.PodName)
		mu.Unlock()
		processed.Add(1)
		return nil
	})

	for i := 0; i < 5; i++ {
		q.Add(newItem("pod-" + string(rune('a'+i))))
	}

	if !waitFor(t, 2*time.Second, func() bool { return processed.Load() == 5 }) {
		t.Fatalf("expected 5 processed, got %d", processed.Load())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 5 {
		t.Fatalf("expected 5 items seen, got %d", len(seen))
	}
}

func TestAddIsNonBlockingWhenFull(t *testing.T) {
	q := New(1)
	q.SetShutdownTimeout(time.Second)

	// Block the (single) worker so the buffer drains slowly.
	release := make(chan struct{})
	var processed atomic.Int64
	q.Start(context.Background(), 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		<-release
		processed.Add(1)
		return nil
	})

	// Fill the buffer (size 1) with two more items; at least one should
	// be dropped with the buffer-full warning.
	q.Add(newItem("a"))
	q.Add(newItem("b"))
	q.Add(newItem("c"))

	// At this point one item is in flight (held by the worker) and at
	// most one is buffered. The third Add must have hit the full path.
	close(release)

	// Wait for the in-flight item to finish.
	if !waitFor(t, 2*time.Second, func() bool { return processed.Load() >= 1 }) {
		t.Fatalf("expected in-flight item processed")
	}
}

func TestNilItemIsIgnored(t *testing.T) {
	q := New(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64
	q.Start(ctx, 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		processed.Add(1)
		return nil
	})

	q.Add(nil)
	q.Add(newItem("real"))

	if !waitFor(t, 2*time.Second, func() bool { return processed.Load() == 1 }) {
		t.Fatalf("expected 1 processed (nil ignored), got %d", processed.Load())
	}
}

func TestProcessFuncErrorIsLoggedNotFatal(t *testing.T) {
	q := New(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64
	q.Start(ctx, 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		processed.Add(1)
		return errors.New("boom")
	})

	q.Add(newItem("a"))
	q.Add(newItem("b"))

	if !waitFor(t, 2*time.Second, func() bool { return processed.Load() == 2 }) {
		t.Fatalf("expected worker to continue after error, got %d", processed.Load())
	}
}

func TestProcessFuncPanicIsRecovered(t *testing.T) {
	q := New(4)
	q.SetShutdownTimeout(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64
	panicOnce := atomic.Bool{}
	q.Start(ctx, 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		n := processed.Add(1)
		if n == 1 && panicOnce.CompareAndSwap(false, true) {
			panic("intentional")
		}
		return nil
	})

	q.Add(newItem("a"))
	q.Add(newItem("b"))

	if !waitFor(t, 2*time.Second, func() bool { return processed.Load() == 2 }) {
		t.Fatalf("expected worker to recover from panic and process 2 items, got %d", processed.Load())
	}
}

func TestStopDrainsInFlightItems(t *testing.T) {
	q := New(8)
	q.SetShutdownTimeout(2 * time.Second)

	hold := make(chan struct{})
	var processed []string
	var mu sync.Mutex
	var started sync.WaitGroup
	started.Add(1)
	// Signal "started" exactly once even though the worker may run
	// multiple items back-to-back. Calling Done more times than Add
	// panics with "negative WaitGroup counter".
	var signalStarted sync.Once

	q.Start(context.Background(), 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		signalStarted.Do(started.Done)
		<-hold // simulate slow work
		mu.Lock()
		processed = append(processed, p.PodName)
		mu.Unlock()
		return nil
	})

	q.Add(newItem("a"))
	q.Add(newItem("b"))
	q.Add(newItem("c"))

	started.Wait() // one item is currently in flight
	close(hold)    // let it complete

	q.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 3 {
		t.Fatalf("expected 3 items processed before Stop returned, got %d (%v)", len(processed), processed)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	q := New(2)
	q.SetShutdownTimeout(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.Start(ctx, 1, func(_ context.Context, _ *detector.PodPendingInfo) error { return nil })
	q.Stop()
	q.Stop() // must not panic / deadlock
}

func TestAddAfterStopIsNoOp(t *testing.T) {
	q := New(2)
	q.SetShutdownTimeout(time.Second)

	q.Start(context.Background(), 1, func(_ context.Context, _ *detector.PodPendingInfo) error { return nil })
	q.Stop()

	// After Stop, Add must not panic (channel is closed).
	q.Add(newItem("post"))
}

func TestSlowProcessFuncDoesNotBlockInformer(t *testing.T) {
	// This is the explicit acceptance criterion from the spec: a slow
	// ProcessFunc must not block the caller of Add. We verify by calling
	// Add from a goroutine and asserting it returns within a small bound
	// even though the worker is stuck.
	q := New(4)
	q.SetShutdownTimeout(2 * time.Second)

	release := make(chan struct{})
	q.Start(context.Background(), 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		<-release
		return nil
	})

	// Occupy the worker.
	q.Add(newItem("first"))

	// Now Add from a goroutine — should return promptly because the
	// channel has capacity and Add is non-blocking.
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		q.Add(newItem("second"))
		q.Add(newItem("third"))
		done <- time.Since(start)
	}()

	select {
	case d := <-done:
		if d > 100*time.Millisecond {
			t.Fatalf("Add blocked for %v; expected near-instant return", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Add did not return within 2s")
	}

	close(release)
}

func TestMultipleWorkersProcessConcurrently(t *testing.T) {
	q := New(16)
	q.SetShutdownTimeout(2 * time.Second)

	const n = 8
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	q.Start(context.Background(), n, func(_ context.Context, _ *detector.PodPendingInfo) error {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		return nil
	})

	for i := 0; i < n; i++ {
		q.Add(newItem("pod"))
	}

	if !waitFor(t, 3*time.Second, func() bool { return maxInFlight.Load() > 1 }) {
		t.Fatalf("expected concurrent processing (maxInFlight > 1), got %d", maxInFlight.Load())
	}
}

func TestContextCancelDrainsRemaining(t *testing.T) {
	q := New(8)
	q.SetShutdownTimeout(2 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	var processed atomic.Int64
	q.Start(ctx, 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		time.Sleep(5 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	for i := 0; i < 5; i++ {
		q.Add(newItem("pod"))
	}
	cancel()
	q.Stop()

	if processed.Load() != 5 {
		t.Fatalf("expected all 5 items processed during drain, got %d", processed.Load())
	}
}

func TestNewDefaultsBufferSize(t *testing.T) {
	q := New(0)
	if got := cap(q.ch); got != DefaultBufferSize {
		t.Fatalf("expected default buffer size %d, got %d", DefaultBufferSize, got)
	}
}

func TestLen(t *testing.T) {
	q := New(8)
	// Block the worker so items accumulate.
	hold := make(chan struct{})
	q.Start(context.Background(), 1, func(_ context.Context, _ *detector.PodPendingInfo) error {
		<-hold
		return nil
	})

	for i := 0; i < 3; i++ {
		q.Add(newItem("p"))
	}

	// One is in flight (held by worker), two are buffered.
	if !waitFor(t, time.Second, func() bool { return q.Len() >= 2 }) {
		t.Fatalf("expected Len >= 2, got %d", q.Len())
	}

	close(hold)
}

func TestProcessFuncNilIsNoOp(t *testing.T) {
	q := New(2)
	q.SetShutdownTimeout(time.Second)
	// Calling Start with nil process must be a safe no-op.
	q.Start(context.Background(), 1, nil)
	q.Add(newItem("p"))
	q.Stop()
}

func TestZeroWorkersDefaultsToOne(t *testing.T) {
	q := New(2)
	q.SetShutdownTimeout(time.Second)

	var processed atomic.Int64
	q.Start(context.Background(), 0, func(_ context.Context, _ *detector.PodPendingInfo) error {
		processed.Add(1)
		return nil
	})

	q.Add(newItem("p"))
	if !waitFor(t, 2*time.Second, func() bool { return processed.Load() == 1 }) {
		t.Fatalf("expected 1 processed (workers defaulted to 1), got %d", processed.Load())
	}
}
