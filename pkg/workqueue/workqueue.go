// Package workqueue provides a small typed FIFO workqueue of
// *detector.PodPendingInfo items. It decouples the informer event handler
// from the (potentially slow) migration creation path.
//
// Design notes:
//   - The queue is backed by a buffered channel. Add is non-blocking: if the
//     buffer is full the item is dropped and a warning is logged. This keeps
//     the informer handler from being throttled by a slow downstream.
//   - A fixed number of worker goroutines dequeue items and invoke the
//     configured ProcessFunc. The migrator is responsible for any per-item
//     deduplication, so the queue itself intentionally does not dedupe —
//     duplicates are cheap and the migrator treats them idempotently.
//   - Stop closes the input channel and waits for workers to drain with a
//     timeout. After Stop returns, further Add calls are no-ops.
package workqueue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"castai-workload-resize-migrator/pkg/detector"
)

const (
	// DefaultBufferSize is the default channel buffer length. Sized so
	// that brief informer bursts (e.g. a controller restart replaying
	// the cache) do not block the event handler.
	DefaultBufferSize = 256

	// DefaultShutdownTimeout bounds how long Stop waits for in-flight
	// items to finish. Tuned to be longer than a typical migration
	// create call but short enough to keep pod termination snappy.
	DefaultShutdownTimeout = 30 * time.Second
)

// ProcessFunc handles a single item dequeued from the queue. It is called
// sequentially per worker; concurrency comes from running multiple workers.
// A non-nil error is logged but does not stop the worker.
type ProcessFunc func(ctx context.Context, item *detector.PodPendingInfo) error

// Queue is a typed workqueue of *detector.PodPendingInfo items.
type Queue struct {
	ch              chan *detector.PodPendingInfo
	process         ProcessFunc
	shutdownTimeout time.Duration

	stopOnce sync.Once
	stopped  chan struct{} // closed when Stop is called; signals Add to drop
	wg       sync.WaitGroup
}

// New constructs a Queue with the given channel buffer size. A non-positive
// value falls back to DefaultBufferSize.
func New(bufferSize int) *Queue {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	return &Queue{
		ch:              make(chan *detector.PodPendingInfo, bufferSize),
		stopped:         make(chan struct{}),
		shutdownTimeout: DefaultShutdownTimeout,
	}
}

// SetShutdownTimeout overrides the drain timeout used by Stop. Intended
// for tests.
func (q *Queue) SetShutdownTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	q.shutdownTimeout = d
}

// Start launches `workers` goroutines that read items from the queue and
// invoke process. The provided context controls worker lifetime: when the
// context is cancelled, each worker drains any remaining items from the
// buffer (using a background context so in-flight items still complete)
// and then exits.
//
// process must be non-nil; if it is nil, Start is a no-op.
func (q *Queue) Start(ctx context.Context, workers int, process ProcessFunc) {
	if process == nil {
		return
	}
	if workers < 1 {
		workers = 1
	}
	q.process = process

	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.runWorker(ctx, i)
	}
}

// Add enqueues an item. It is non-blocking: nil items are silently
// ignored, items enqueued after Stop are dropped, and items added when
// the buffer is full are dropped with a warning so the informer handler
// is never throttled by a slow downstream consumer.
func (q *Queue) Add(item *detector.PodPendingInfo) {
	if item == nil {
		return
	}

	// Fast-path: already shutting down — drop without logging.
	select {
	case <-q.stopped:
		return
	default:
	}

	// recover guards against a race with Stop: between the fast-path
	// check and the select below, Stop may close q.ch. Sending on a
	// closed channel would otherwise panic.
	defer func() {
		if r := recover(); r != nil {
			// Channel was closed mid-send; drop silently.
			_ = r
		}
	}()

	select {
	case q.ch <- item:
		return
	case <-q.stopped:
		return
	default:
		slog.Warn("workqueue buffer full; dropping item",
			"pod", item.Namespace+"/"+item.PodName,
			"bufferSize", cap(q.ch),
		)
	}
}

// Stop signals workers to exit, closes the input channel, and waits for
// in-flight items to finish (bounded by the shutdown timeout). Stop is
// safe to call multiple times and from any goroutine.
func (q *Queue) Stop() {
	q.stopOnce.Do(func() {
		close(q.stopped)
		close(q.ch)
	})

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(q.shutdownTimeout):
		slog.Warn("workqueue shutdown timed out; some workers may still be running",
			"timeout", q.shutdownTimeout,
		)
	}
}

// Len returns the current number of items buffered in the queue. Useful
// for tests and metrics.
func (q *Queue) Len() int {
	return len(q.ch)
}

// runWorker is the main loop for a single worker goroutine. It processes
// items until either the channel is closed (Stop) or the context is
// cancelled; in the latter case it drains whatever remains.
func (q *Queue) runWorker(ctx context.Context, id int) {
	defer q.wg.Done()
	slog.Debug("workqueue worker started", "id", id)

	for {
		select {
		case <-ctx.Done():
			q.drain(id)
			return
		case item, ok := <-q.ch:
			if !ok {
				slog.Debug("workqueue worker exiting (channel closed)", "id", id)
				return
			}
			if item == nil {
				continue
			}
			q.processItem(ctx, id, item)
		}
	}
}

// drain processes any items remaining in the buffer and exits. Items are
// processed with a background context so cancellation of the original
// context does not abort in-flight work — the migrator needs to finish
// what it started.
func (q *Queue) drain(id int) {
	slog.Debug("workqueue worker draining", "id", id)
	for {
		select {
		case item, ok := <-q.ch:
			if !ok {
				return
			}
			if item == nil {
				continue
			}
			q.processItem(context.Background(), id, item)
		default:
			return
		}
	}
}

// processItem invokes the configured ProcessFunc and logs any error or
// panic. Panics are recovered so a single bad item cannot kill the worker.
func (q *Queue) processItem(ctx context.Context, id int, item *detector.PodPendingInfo) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("workqueue worker panicked",
				"id", id,
				"pod", item.Namespace+"/"+item.PodName,
				"panic", r,
			)
		}
	}()
	if err := q.process(ctx, item); err != nil {
		slog.Error("workqueue process failed",
			"id", id,
			"pod", item.Namespace+"/"+item.PodName,
			"error", err,
		)
	}
}
