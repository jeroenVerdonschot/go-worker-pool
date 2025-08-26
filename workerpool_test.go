package workerpool

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

type testProcessor struct {
	id   string
	fail bool
}

func (tp *testProcessor) Process(ctx context.Context) error {
	if tp.fail {
		return fmt.Errorf("job %s failed", tp.id)
	}
	return nil
}

func (tp *testProcessor) GetID() string {
	return tp.id
}

func TestPool(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fmt.Println(logger)

	pool := NewPool(5, 10, 3, logger)

	for i := 0; i < 10; i++ {
		job := &testProcessor{
			id:   fmt.Sprintf("job-%d", i),
			fail: i%5 == 0, // Every 5th job fails
		}
		if err := pool.Add(job); err != nil {
			t.Fatalf("failed to add job: %v", err)
		}
	}

	pool.Start()
	pool.Wait()
	fmt.Println("All jobs processed")
}
