package filesystem

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	metastorage "schneider.vip/retryspool/storage/meta"
)

func TestCAS_ConcurrentMoveToState(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "retryspool-meta-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	ctx := context.Background()
	messageID := "cas-msg"

	// Create initial message in Incoming state
	err = backend.StoreMeta(ctx, messageID, metastorage.MessageMetadata{
		ID:    messageID,
		State: metastorage.StateIncoming,
	})
	if err != nil {
		t.Fatal(err)
	}

	const workerCount = 100
	var wg sync.WaitGroup
	wg.Add(workerCount)

	successCount := 0
	conflictCount := 0
	var mu sync.Mutex

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			err := backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateActive)
			mu.Lock()
			if err == nil {
				successCount++
			} else if err == metastorage.ErrStateConflict {
				conflictCount++
			} else {
				t.Errorf("Unexpected error: %v", err)
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Errorf("Expected exactly 1 success, got %d", successCount)
	}
	if conflictCount != workerCount-1 {
		t.Errorf("Expected %d conflicts, got %d", workerCount-1, conflictCount)
	}

	// Verify final state
	meta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.State != metastorage.StateActive {
		t.Errorf("Expected final state Active, got %s", meta.State)
	}
}

func TestCAS_WrongFromState(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "retryspool-meta-test-*")
	defer os.RemoveAll(tempDir)
	backend, _ := NewBackend(tempDir)
	defer backend.Close()

	ctx := context.Background()
	messageID := "wrong-state-msg"

	backend.StoreMeta(ctx, messageID, metastorage.MessageMetadata{
		ID:    messageID,
		State: metastorage.StateIncoming,
	})

	// Try to move from Active to Deferred, but message is Incoming
	err := backend.MoveToState(ctx, messageID, metastorage.StateActive, metastorage.StateDeferred)
	if err != metastorage.ErrStateConflict {
		t.Errorf("Expected ErrStateConflict, got %v", err)
	}
}

func TestCAS_DoubleMove(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "retryspool-meta-test-*")
	defer os.RemoveAll(tempDir)
	backend, _ := NewBackend(tempDir)
	defer backend.Close()

	ctx := context.Background()
	messageID := "double-move"

	backend.StoreMeta(ctx, messageID, metastorage.MessageMetadata{
		ID:    messageID,
		State: metastorage.StateIncoming,
	})

	// First move: success
	err := backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateActive)
	if err != nil {
		t.Errorf("First move failed: %v", err)
	}

	// Second move: conflict
	err = backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateActive)
	if err != metastorage.ErrStateConflict {
		t.Errorf("Second move expected ErrStateConflict, got %v", err)
	}
}

func TestCAS_MoveAndRead(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "retryspool-meta-test-*")
	defer os.RemoveAll(tempDir)
	backend, _ := NewBackend(tempDir)
	defer backend.Close()

	ctx := context.Background()
	messageID := "move-read"
	backend.StoreMeta(ctx, messageID, metastorage.MessageMetadata{
		ID:    messageID,
		State: metastorage.StateIncoming,
	})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateActive)
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			meta, err := backend.GetMeta(ctx, messageID)
			if err == nil {
				if meta.State != metastorage.StateIncoming && meta.State != metastorage.StateActive {
					t.Errorf("Invalid intermediate state: %s", meta.State)
				}
			}
		}
	}()

	wg.Wait()
}

func BenchmarkConcurrentMoveToState(b *testing.B) {
	tempDir, _ := os.MkdirTemp("", "retryspool-bench-meta-*")
	defer os.RemoveAll(tempDir)
	backend, _ := NewBackend(tempDir)
	defer backend.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msgID := fmt.Sprintf("bench-msg-%d-%d", i, os.Getpid())
			backend.StoreMeta(ctx, msgID, metastorage.MessageMetadata{State: metastorage.StateIncoming})
			backend.MoveToState(ctx, msgID, metastorage.StateIncoming, metastorage.StateActive)
			i++
		}
	})
}
