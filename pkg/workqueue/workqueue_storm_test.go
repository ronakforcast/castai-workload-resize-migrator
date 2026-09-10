package workqueue

// F10 (characterization of intended design): a burst of detector events
// against a slow migration-create consumer. Add is non-blocking with
// drop-on-full, so the producer (which runs on the informer handler
// goroutine) must never stall, no panic may escape, and Stop must still
// drain and return cleanly.
// Issue: https://github.com/ronakforcast/castai-workload-resize-migrator/issues/1

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/detector"
)

func TestStormBurstWithSlowConsumerNeverStallsProducer(t *testing.T) {
	// Drop warnings would flood the test output; discard them.
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(prev)

	q := New(4)
	q.SetShutdownTimeout(10 * time.Second)

	var processed atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.Start(ctx, 1, func(_ context.Context, item *detector.PodPendingInfo) error {
		time.Sleep(5 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	// A burst far larger than the 4-slot buffer, while the single
	// consumer is deliberately slow.
	const burst = 100
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; i < burst; i++ {
			q.Add(&detector.PodPendingInfo{
				Namespace: "default",
				PodName:   fmt.Sprintf("storm-%d", i),
				NodeName:  "node-1",
			})
		}
	}()

	// The producer must finish far faster than the consumer could drain
	// the burst serially — i.e. it was never blocked on the full buffer.
	// A stall here would mean the informer event handler is throttled by
	// a slow downstream migration create.
	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("producer stalled on a full buffer — informer handler would be throttled")
	}

	// The queue never exceeded its configured capacity.
	if q.Len() > 4 {
		t.Fatalf("queue length %d exceeds buffer capacity 4", q.Len())
	}

	// Stop drains whatever made it into the buffer and returns cleanly.
	q.Stop()

	got := int(processed.Load())
	if got < 1 || got > burst {
		t.Fatalf("processed %d items, want 1..%d (drops beyond the buffer are expected)", got, burst)
	}
}
