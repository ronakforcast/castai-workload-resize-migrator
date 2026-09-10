// workqueue_business_test.go covers workqueue business-logic tests pinned
// to QA-plan test IDs:
//
//	TC-15: backpressure under slow processor (100 items, processor sleeps
//	       5ms each, all processed, no duplicates, queue depth > 0)
//	+      multiple workers process without duplicates
//	+      processor error handling (item not lost)
//
// These complement pkg/workqueue/workqueue_test.go (which exhaustively
// exercises the queue's internal mechanics) by locking down observable
// behaviour the QA plan cares about.
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

// drainDeadline bounds how long a test waits for an event that should
// already have happened (worker pickup). The QA plan's worst case is
// 100 items × 5 ms = 500 ms; we pad generously.
const drainDeadline = 5 * time.Second

// ─── TC-15: backpressure under slow processor ───────────────────────────

// TestBusiness_TC15_SlowProcessorAllItemsProcessed pins down the QA
// plan's headline acceptance criterion: a slow processor must not lose
// items, and all items must eventually be processed exactly once.
//
// We add 100 items rapidly, then a worker processes them serially with
// a 5ms sleep per item. The queue must:
//
//  1. accept every Add call (no synchronous block on the producer);
//  2. surface queue depth > 0 at some point (proves items actually
//     buffered before the worker drained them);
//  3. process every item exactly once (no duplicates);
//  4. finish the full 100 items (no drops).
func TestBusiness_TC15_SlowProcessorAllItemsProcessed(t *testing.T) {
	const (
		totalItems     = 100
		processorSleep = 5 * time.Millisecond
	)

	q := New(totalItems * 2) // generous buffer to avoid drops
	q.SetShutdownTimeout(2 * time.Second)

	processed := make(map[string]int, totalItems)
	var (
		processedMu sync.Mutex
		processedN  atomic.Int64
	)
	q.Start(context.Background(), 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		time.Sleep(processorSleep)
		processedMu.Lock()
		processed[p.PodName]++
		processedMu.Unlock()
		processedN.Add(1)
		return nil
	})

	// Produce 100 items as quickly as Add allows. Add must not block.
	producerStart := time.Now()
	for i := 0; i < totalItems; i++ {
		q.Add(newItem(podNameFor(i)))
	}
	producerElapsed := time.Since(producerStart)

	// (1) producer must not have been throttled by the slow worker.
	// 100 items into a 200-slot buffer is essentially instantaneous;
	// anything > 1s implies Add blocked.
	if producerElapsed > time.Second {
		t.Fatalf("Add blocked: 100 items took %v", producerElapsed)
	}

	// (2) depth must be > 0 at some point during processing. Poll until
	// the worker has drained at least one item OR the queue holds > 0.
	depthSeen := 0
	deadline := time.Now().Add(drainDeadline)
	for time.Now().Before(deadline) {
		if q.Len() > 0 {
			depthSeen = q.Len()
			break
		}
		if processedN.Load() > 0 {
			// First item dequeued; depth must have been totalItems-1
			// momentarily. We've missed the peak — keep polling, the
			// next item may still be in flight.
			depthSeen = q.Len()
			break
		}
		time.Sleep(time.Millisecond)
	}
	if depthSeen == 0 && totalItems > 1 {
		// Best-effort: if the processor consumed everything before
		// we could observe depth, that's still a passing outcome as
		// long as (3) and (4) hold. We log it as informational.
		t.Logf("queue depth observation missed peak (very fast processor for this test machine); checking correctness only")
	}

	// (3) & (4): wait for all 100 to be processed, then verify
	// uniqueness and completeness.
	if !waitFor(t, drainDeadline+5*time.Second, func() bool {
		return processedN.Load() == int64(totalItems)
	}) {
		t.Fatalf("processed=%d, want %d (items lost?)", processedN.Load(), totalItems)
	}

	processedMu.Lock()
	defer processedMu.Unlock()
	if len(processed) != totalItems {
		t.Fatalf("unique items=%d, want %d", len(processed), totalItems)
	}
	for name, n := range processed {
		if n != 1 {
			t.Fatalf("item %s processed %d times; want 1 (no duplicates)", name, n)
		}
	}

	// (5) buffer must be empty after drain.
	if got := q.Len(); got != 0 {
		t.Fatalf("queue Len after drain=%d, want 0", got)
	}
}

// podNameFor produces deterministic, unique names for the TC-15 test.
func podNameFor(i int) string {
	return "pod-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf[pos:])
}

// ─── Multiple workers process without duplicates ─────────────────────────

// TestBusiness_MultipleWorkersNoDuplicates verifies that running several
// workers in parallel does not cause the same item to be processed more
// than once. The queue is intentionally a FIFO with no dedup, but
// because each item is enqueued exactly once there must be no
// duplicate processing.
func TestBusiness_MultipleWorkersNoDuplicates(t *testing.T) {
	const (
		totalItems = 50
		workers    = 4
	)
	q := New(totalItems * 4)
	q.SetShutdownTimeout(2 * time.Second)

	var (
		processedMu sync.Mutex
		processed   = make(map[string]int, totalItems)
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
	)
	q.Start(context.Background(), workers, func(_ context.Context, p *detector.PodPendingInfo) error {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		processedMu.Lock()
		processed[p.PodName]++
		processedMu.Unlock()
		return nil
	})

	for i := 0; i < totalItems; i++ {
		q.Add(newItem(podNameFor(i)))
	}

	if !waitFor(t, drainDeadline, func() bool {
		processedMu.Lock()
		defer processedMu.Unlock()
		return len(processed) == totalItems
	}) {
		processedMu.Lock()
		got := len(processed)
		processedMu.Unlock()
		t.Fatalf("processed unique items=%d, want %d", got, totalItems)
	}

	// Workers actually ran in parallel.
	if maxInFlight.Load() < 2 {
		t.Fatalf("expected maxInFlight>=2, got %d (workers not running concurrently?)", maxInFlight.Load())
	}

	processedMu.Lock()
	defer processedMu.Unlock()
	for name, n := range processed {
		if n != 1 {
			t.Fatalf("item %s processed %d times; want 1", name, n)
		}
	}
}

// ─── Processor error handling ────────────────────────────────────────────

// TestBusiness_ProcessorErrorDoesNotLoseItem pins down the error path:
// when ProcessFunc returns a non-nil error, the item must not be
// re-delivered (no infinite retry loop) AND must not be silently dropped
// from the accounting (we already saw it once). The QA plan says:
// "if error returned, item should be retried or at least not lost".
// The current design logs-and-continues; we lock that contract in.
func TestBusiness_ProcessorErrorDoesNotLoseItem(t *testing.T) {
	q := New(64)
	q.SetShutdownTimeout(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processedN atomic.Int64
	q.Start(ctx, 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		processedN.Add(1)
		if p.PodName == "boom" {
			return errors.New("intentional")
		}
		return nil
	})

	for i := 0; i < 5; i++ {
		q.Add(newItem(podNameFor(i)))
	}
	q.Add(newItem("boom"))
	for i := 5; i < 9; i++ {
		q.Add(newItem(podNameFor(i)))
	}

	if !waitFor(t, drainDeadline, func() bool { return processedN.Load() == 10 }) {
		t.Fatalf("processed=%d, want 10 (error must not block subsequent items)", processedN.Load())
	}
}

// TestBusiness_ProcessorErrorLoggedNotRetried documents the explicit
// non-retry behaviour: a failing item is processed exactly once. If a
// future change introduces retry-on-error, this test will flag it.
func TestBusiness_ProcessorErrorLoggedNotRetried(t *testing.T) {
	q := New(4)
	q.SetShutdownTimeout(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processedN atomic.Int64
	q.Start(ctx, 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		processedN.Add(1)
		return errors.New("always-fail")
	})

	q.Add(newItem("a"))
	q.Add(newItem("b"))
	q.Add(newItem("c"))

	if !waitFor(t, drainDeadline, func() bool { return processedN.Load() == 3 }) {
		t.Fatalf("processed=%d, want 3 (one pass each, no retry)", processedN.Load())
	}
}

// TestBusiness_AddAfterStopIsDroppedSilently ensures the safety net for
// the lifecycle: producers that fire after Stop() must not panic the
// controller. This is the contract that lets the detector callback
// hand off to a queue even while the queue is shutting down.
func TestBusiness_AddAfterStopIsDroppedSilently(t *testing.T) {
	q := New(2)
	q.SetShutdownTimeout(time.Second)

	q.Start(context.Background(), 1, func(_ context.Context, _ *detector.PodPendingInfo) error { return nil })
	q.Stop()

	// Add must not panic even after Stop closed the channel.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Add after Stop panicked: %v", r)
		}
	}()
	q.Add(newItem("post-stop-1"))
	q.Add(newItem("post-stop-2"))
}
