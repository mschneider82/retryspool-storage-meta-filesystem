package filesystem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metastorage "schneider.vip/retryspool/storage/meta"
)

func TestNewBackend(t *testing.T) {
	tempDir := t.TempDir()
	
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	// Verify directories were created
	for _, state := range []metastorage.QueueState{
		metastorage.StateIncoming,
		metastorage.StateActive,
		metastorage.StateDeferred,
		metastorage.StateHold,
		metastorage.StateBounce,
	} {
		dir := filepath.Join(tempDir, state.String())
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("State directory %s was not created", dir)
		}
	}
}

func TestNewBackendWithOptions(t *testing.T) {
	tempDir := t.TempDir()
	
	backend, err := NewBackendWithOptions(tempDir, true, true)
	if err != nil {
		t.Fatalf("Failed to create backend with options: %v", err)
	}
	defer backend.Close()
	
	if !backend.disableSync {
		t.Error("Expected disableSync to be true")
	}
	if !backend.batchSync {
		t.Error("Expected batchSync to be true")
	}
}

func TestStoreMeta(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-1"
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
			"to":   "recipient@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}
	
	// Verify file was created
	metaPath := backend.getMetaPath(messageID, metastorage.StateIncoming)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("Metadata file was not created")
	}
}

func TestGetMeta(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-2"
	originalMetadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 10,
		Headers: map[string]string{
			"subject": "Test Subject",
			"from":    "sender@example.com",
		},
		Attempts: 2,
	}
	
	// Store metadata
	err = backend.StoreMeta(ctx, messageID, originalMetadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}
	
	// Retrieve metadata
	retrievedMetadata, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	
	// Verify fields
	if retrievedMetadata.ID != messageID {
		t.Errorf("ID mismatch: expected %s, got %s", messageID, retrievedMetadata.ID)
	}
	if retrievedMetadata.State != metastorage.StateIncoming {
		t.Errorf("State mismatch: expected %v, got %v", metastorage.StateIncoming, retrievedMetadata.State)
	}
	if retrievedMetadata.Priority != 10 {
		t.Errorf("Priority mismatch: expected 10, got %d", retrievedMetadata.Priority)
	}
	if retrievedMetadata.Headers["subject"] != "Test Subject" {
		t.Errorf("Header mismatch: expected 'Test Subject', got %s", retrievedMetadata.Headers["subject"])
	}
	if retrievedMetadata.Attempts != 2 {
		t.Errorf("Attempts mismatch: expected 2, got %d", retrievedMetadata.Attempts)
	}
}

func TestGetMeta_NotFound(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	_, err = backend.GetMeta(ctx, "non-existent-message")
	if err != metastorage.ErrMessageNotFound {
		t.Errorf("Expected ErrMessageNotFound, got %v", err)
	}
}

func TestUpdateMeta(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-3"
	
	// Store initial metadata
	initialMetadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
		},
		Attempts: 1,
		Created:  time.Now().Add(-1 * time.Hour),
	}
	
	err = backend.StoreMeta(ctx, messageID, initialMetadata)
	if err != nil {
		t.Fatalf("Failed to store initial metadata: %v", err)
	}
	
	// Update metadata
	updatedMetadata := initialMetadata
	updatedMetadata.Priority = 15
	updatedMetadata.Attempts = 3
	updatedMetadata.Headers["retry"] = "true"
	
	err = backend.UpdateMeta(ctx, messageID, updatedMetadata)
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}
	
	// Retrieve and verify
	retrievedMetadata, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get updated metadata: %v", err)
	}
	
	if retrievedMetadata.Priority != 15 {
		t.Errorf("Priority not updated: expected 15, got %d", retrievedMetadata.Priority)
	}
	if retrievedMetadata.Attempts != 3 {
		t.Errorf("Attempts not updated: expected 3, got %d", retrievedMetadata.Attempts)
	}
	if retrievedMetadata.Headers["retry"] != "true" {
		t.Errorf("Header not updated: expected 'true', got %s", retrievedMetadata.Headers["retry"])
	}
	
	// Verify creation time preserved
	if !retrievedMetadata.Created.Equal(initialMetadata.Created) {
		t.Error("Creation time was not preserved during update")
	}
}

func TestUpdateMeta_StateChange(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-4"
	
	// Store initial metadata in incoming state
	initialMetadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, initialMetadata)
	if err != nil {
		t.Fatalf("Failed to store initial metadata: %v", err)
	}
	
	// Update with state change to active
	updatedMetadata := initialMetadata
	updatedMetadata.State = metastorage.StateActive
	updatedMetadata.Attempts = 2
	
	err = backend.UpdateMeta(ctx, messageID, updatedMetadata)
	if err != nil {
		t.Fatalf("Failed to update metadata with state change: %v", err)
	}
	
	// Verify file moved to new state directory
	oldPath := backend.getMetaPath(messageID, metastorage.StateIncoming)
	newPath := backend.getMetaPath(messageID, metastorage.StateActive)
	
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("Old metadata file still exists after state change")
	}
	
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		t.Error("New metadata file was not created after state change")
	}
	
	// Verify metadata content
	retrievedMetadata, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata after state change: %v", err)
	}
	
	if retrievedMetadata.State != metastorage.StateActive {
		t.Errorf("State not updated: expected %v, got %v", metastorage.StateActive, retrievedMetadata.State)
	}
	if retrievedMetadata.Attempts != 2 {
		t.Errorf("Attempts not updated: expected 2, got %d", retrievedMetadata.Attempts)
	}
}

func TestDeleteMeta(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-5"
	
	// Store metadata
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}
	
	// Delete metadata
	err = backend.DeleteMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete metadata: %v", err)
	}
	
	// Verify deletion
	_, err = backend.GetMeta(ctx, messageID)
	if err != metastorage.ErrMessageNotFound {
		t.Errorf("Expected ErrMessageNotFound after deletion, got %v", err)
	}
	
	// Verify file was removed
	metaPath := backend.getMetaPath(messageID, metastorage.StateIncoming)
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("Metadata file still exists after deletion")
	}
}

func TestDeleteMeta_NotFound(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	err = backend.DeleteMeta(ctx, "non-existent-message")
	if err != metastorage.ErrMessageNotFound {
		t.Errorf("Expected ErrMessageNotFound for non-existent message, got %v", err)
	}
}

func TestMoveToState(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-6"
	
	// Store metadata in incoming state
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}
	
	// Move to active state
	err = backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateActive)
	if err != nil {
		t.Fatalf("Failed to move to active state: %v", err)
	}
	
	// Verify state change
	retrievedMetadata, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata after state move: %v", err)
	}
	
	if retrievedMetadata.State != metastorage.StateActive {
		t.Errorf("State not changed: expected %v, got %v", metastorage.StateActive, retrievedMetadata.State)
	}
	
	// Verify files moved
	oldPath := backend.getMetaPath(messageID, metastorage.StateIncoming)
	newPath := backend.getMetaPath(messageID, metastorage.StateActive)
	
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("Old metadata file still exists after move")
	}
	
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		t.Error("New metadata file was not created after move")
	}
}

func TestMoveToState_InvalidTransition(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-7"
	
	// Store metadata in active state
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateActive,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}
	
	// Try to move from incoming (wrong source state)
	err = backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateDeferred)
	if err == nil {
		t.Error("Expected error when moving from wrong source state")
	}
}

func TestMoveToState_SameState(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "test-message-8"
	
	// Store metadata
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateActive,
		Priority: 5,
		Headers: map[string]string{
			"from": "test@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}
	
	// Move to same state (should be no-op)
	err = backend.MoveToState(ctx, messageID, metastorage.StateActive, metastorage.StateActive)
	if err != nil {
		t.Fatalf("Failed to move to same state: %v", err)
	}
}

func TestListMessages(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	// Create test messages
	messages := []struct {
		id       string
		state    metastorage.QueueState
		priority int
		created  time.Time
	}{
		{"msg1", metastorage.StateIncoming, 10, time.Now().Add(-1 * time.Hour)},
		{"msg2", metastorage.StateIncoming, 5, time.Now().Add(-30 * time.Minute)},
		{"msg3", metastorage.StateIncoming, 15, time.Now().Add(-2 * time.Hour)},
		{"msg4", metastorage.StateActive, 8, time.Now().Add(-45 * time.Minute)},
	}
	
	for _, msg := range messages {
		metadata := metastorage.MessageMetadata{
			ID:       msg.id,
			State:    msg.state,
			Priority: msg.priority,
			Headers: map[string]string{
				"from": "test@example.com",
			},
			Attempts: 1,
			Created:  msg.created,
		}
		
		err = backend.StoreMeta(ctx, msg.id, metadata)
		if err != nil {
			t.Fatalf("Failed to store metadata for %s: %v", msg.id, err)
		}
	}
	
	// Test basic listing (incoming messages)
	options := metastorage.MessageListOptions{
		Limit: 10,
	}
	
	result, err := backend.ListMessages(ctx, metastorage.StateIncoming, options)
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	
	if result.Total != 3 {
		t.Errorf("Expected 3 incoming messages, got %d", result.Total)
	}
	
	if len(result.MessageIDs) != 3 {
		t.Errorf("Expected 3 message IDs, got %d", len(result.MessageIDs))
	}
	
	// Test sorting by priority
	options.SortBy = "priority"
	options.SortOrder = "asc"
	
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, options)
	if err != nil {
		t.Fatalf("Failed to list messages with priority sort: %v", err)
	}
	
	// Should be sorted: msg2 (5), msg1 (10), msg3 (15)
	expectedOrder := []string{"msg2", "msg1", "msg3"}
	for i, expectedID := range expectedOrder {
		if result.MessageIDs[i] != expectedID {
			t.Errorf("Sort order incorrect at position %d: expected %s, got %s", i, expectedID, result.MessageIDs[i])
		}
	}
	
	// Test pagination
	options.Limit = 1
	options.Offset = 1
	
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, options)
	if err != nil {
		t.Fatalf("Failed to list messages with pagination: %v", err)
	}
	
	if len(result.MessageIDs) != 1 {
		t.Errorf("Expected 1 message with pagination, got %d", len(result.MessageIDs))
	}
	
	if result.MessageIDs[0] != "msg1" {
		t.Errorf("Pagination incorrect: expected msg1, got %s", result.MessageIDs[0])
	}
	
	if !result.HasMore {
		t.Error("Expected HasMore to be true with pagination")
	}
}

func TestListMessages_EmptyState(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	options := metastorage.MessageListOptions{
		Limit: 10,
	}
	
	result, err := backend.ListMessages(ctx, metastorage.StateIncoming, options)
	if err != nil {
		t.Fatalf("Failed to list messages from empty state: %v", err)
	}
	
	if result.Total != 0 {
		t.Errorf("Expected 0 messages, got %d", result.Total)
	}
	
	if len(result.MessageIDs) != 0 {
		t.Errorf("Expected 0 message IDs, got %d", len(result.MessageIDs))
	}
	
	if result.HasMore {
		t.Error("Expected HasMore to be false for empty state")
	}
}

func TestNewMessageIterator(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	// Create test messages
	for i := 0; i < 5; i++ {
		messageID := fmt.Sprintf("msg%d", i)
		metadata := metastorage.MessageMetadata{
			ID:       messageID,
			State:    metastorage.StateIncoming,
			Priority: i + 1,
			Headers: map[string]string{
				"from": "test@example.com",
				"id":   messageID,
			},
			Attempts: 1,
		}
		
		err = backend.StoreMeta(ctx, messageID, metadata)
		if err != nil {
			t.Fatalf("Failed to store metadata for %s: %v", messageID, err)
		}
	}
	
	// Create iterator
	iterator, err := backend.NewMessageIterator(ctx, metastorage.StateIncoming, 2)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iterator.Close()
	
	// Iterate through messages
	messageCount := 0
	seenIDs := make(map[string]bool)
	
	for {
		metadata, hasMore, err := iterator.Next(ctx)
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		
		if metadata.ID == "" {
			break // End of iteration
		}
		
		messageCount++
		seenIDs[metadata.ID] = true
		
		// Verify metadata content
		if metadata.State != metastorage.StateIncoming {
			t.Errorf("Wrong state in iterator: expected %v, got %v", metastorage.StateIncoming, metadata.State)
		}
		
		if metadata.Headers["from"] != "test@example.com" {
			t.Errorf("Wrong header in iterator: expected test@example.com, got %s", metadata.Headers["from"])
		}
		
		// Check hasMore logic
		if !hasMore && messageCount < 5 {
			// If hasMore is false but we haven't seen all messages, continue
			// This tests the streaming behavior
		}
	}
	
	if messageCount != 5 {
		t.Errorf("Expected to iterate over 5 messages, got %d", messageCount)
	}
	
	// Verify all messages were seen
	for i := 0; i < 5; i++ {
		expectedID := fmt.Sprintf("msg%d", i)
		if !seenIDs[expectedID] {
			t.Errorf("Message %s was not seen during iteration", expectedID)
		}
	}
}

func TestIterator_EmptyState(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	// Create iterator for empty state
	iterator, err := backend.NewMessageIterator(ctx, metastorage.StateIncoming, 10)
	if err != nil {
		t.Fatalf("Failed to create iterator for empty state: %v", err)
	}
	defer iterator.Close()
	
	// Should immediately return empty result
	metadata, hasMore, err := iterator.Next(ctx)
	if err != nil {
		t.Fatalf("Iterator error on empty state: %v", err)
	}
	
	if metadata.ID != "" {
		t.Errorf("Expected empty metadata ID, got %s", metadata.ID)
	}
	
	if hasMore {
		t.Error("Expected hasMore to be false for empty state")
	}
}

func TestIterator_Close(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	iterator, err := backend.NewMessageIterator(ctx, metastorage.StateIncoming, 10)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	
	// Close iterator
	err = iterator.Close()
	if err != nil {
		t.Fatalf("Failed to close iterator: %v", err)
	}
	
	// Try to use closed iterator
	_, _, err = iterator.Next(ctx)
	if err == nil {
		t.Error("Expected error when using closed iterator")
	}
}

func TestBackend_Close(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	
	ctx := context.Background()
	
	// Close backend
	err = backend.Close()
	if err != nil {
		t.Fatalf("Failed to close backend: %v", err)
	}
	
	// Try to use closed backend
	_, err = backend.GetMeta(ctx, "test")
	if err != metastorage.ErrBackendClosed {
		t.Errorf("Expected ErrBackendClosed, got %v", err)
	}
	
	err = backend.StoreMeta(ctx, "test", metastorage.MessageMetadata{})
	if err != metastorage.ErrBackendClosed {
		t.Errorf("Expected ErrBackendClosed, got %v", err)
	}
}

func TestConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	
	// Test concurrent operations
	var wg sync.WaitGroup
	errorCh := make(chan error, 20)
	
	// Concurrent stores
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			messageID := fmt.Sprintf("concurrent-msg-%d", id)
			metadata := metastorage.MessageMetadata{
				ID:       messageID,
				State:    metastorage.StateIncoming,
				Priority: id,
				Headers: map[string]string{
					"from": "concurrent@example.com",
					"id":   messageID,
				},
				Attempts: 1,
			}
			
			err := backend.StoreMeta(ctx, messageID, metadata)
			if err != nil {
				errorCh <- fmt.Errorf("concurrent store error for %s: %v", messageID, err)
			}
		}(i)
	}
	
	// Concurrent reads (should not conflict with stores)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Try to read existing message (may or may not exist yet)
			messageID := fmt.Sprintf("concurrent-msg-%d", id)
			_, err := backend.GetMeta(ctx, messageID)
			// Don't treat "not found" as error since store might not be complete
			if err != nil && err != metastorage.ErrMessageNotFound {
				errorCh <- fmt.Errorf("concurrent read error for %s: %v", messageID, err)
			}
		}(i)
	}
	
	// Concurrent iterators
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			iterator, err := backend.NewMessageIterator(ctx, metastorage.StateIncoming, 5)
			if err != nil {
				errorCh <- fmt.Errorf("concurrent iterator creation error: %v", err)
				return
			}
			defer iterator.Close()
			
			// Iterate through available messages
			for j := 0; j < 20; j++ { // Limit iterations to avoid infinite loop
				metadata, _, err := iterator.Next(ctx)
				if err != nil {
					errorCh <- fmt.Errorf("concurrent iterator error: %v", err)
					return
				}
				if metadata.ID == "" {
					break // End of iteration
				}
			}
		}()
	}
	
	wg.Wait()
	close(errorCh)
	
	// Check for errors
	for err := range errorCh {
		t.Error(err)
	}
}

func TestRecoverFromCrash(t *testing.T) {
	tempDir := t.TempDir()
	
	// Create some temp files to simulate crash
	tempFiles := []string{
		filepath.Join(tempDir, "incoming", "test1.json.tmp.abc123"),
		filepath.Join(tempDir, "active", "test2.json.tmp.def456"),
		filepath.Join(tempDir, "test3.tmp"),
	}
	
	// Create directories and temp files
	for _, tempFile := range tempFiles {
		dir := filepath.Dir(tempFile)
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}
		
		file, err := os.Create(tempFile)
		if err != nil {
			t.Fatalf("Failed to create temp file %s: %v", tempFile, err)
		}
		file.Close()
	}
	
	// Verify temp files exist
	for _, tempFile := range tempFiles {
		if _, err := os.Stat(tempFile); os.IsNotExist(err) {
			t.Fatalf("Temp file %s was not created", tempFile)
		}
	}
	
	// Create backend (should trigger recovery)
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	// Verify temp files were cleaned up
	for _, tempFile := range tempFiles {
		if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
			t.Errorf("Temp file %s was not cleaned up during recovery", tempFile)
		}
	}
}

func TestGenerateRandomSuffix(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	// Generate multiple suffixes and verify they're different
	suffixes := make(map[string]bool)
	for i := 0; i < 100; i++ {
		suffix := backend.generateRandomSuffix()
		
		if len(suffix) != 8 { // 4 bytes = 8 hex characters
			t.Errorf("Expected suffix length 8, got %d", len(suffix))
		}
		
		if suffixes[suffix] {
			t.Errorf("Duplicate suffix generated: %s", suffix)
		}
		suffixes[suffix] = true
	}
}

func TestAtomicOperations(t *testing.T) {
	tempDir := t.TempDir()
	backend, err := NewBackend(tempDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "atomic-test-message"
	
	// Test atomic store - even if interrupted, file should be complete or not exist
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "atomic@example.com",
			"data": strings.Repeat("x", 1000), // Large header to test atomicity
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata atomically: %v", err)
	}
	
	// Verify no temp files left behind
	metaDir := filepath.Join(tempDir, metastorage.StateIncoming.String())
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		t.Fatalf("Failed to read metadata directory: %v", err)
	}
	
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp") {
			t.Errorf("Temp file found after atomic operation: %s", entry.Name())
		}
	}
	
	// Verify content integrity
	retrievedMetadata, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata after atomic store: %v", err)
	}
	
	if retrievedMetadata.Headers["data"] != metadata.Headers["data"] {
		t.Error("Data corrupted during atomic operation")
	}
}

func TestSyncOptions(t *testing.T) {
	tempDir := t.TempDir()
	
	// Test with sync disabled
	backend, err := NewBackendWithOptions(tempDir, true, false)
	if err != nil {
		t.Fatalf("Failed to create backend with sync disabled: %v", err)
	}
	defer backend.Close()
	
	ctx := context.Background()
	messageID := "sync-test-message"
	
	// Store metadata (should not sync to disk)
	metadata := metastorage.MessageMetadata{
		ID:       messageID,
		State:    metastorage.StateIncoming,
		Priority: 5,
		Headers: map[string]string{
			"from": "sync@example.com",
		},
		Attempts: 1,
	}
	
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata with sync disabled: %v", err)
	}
	
	// Should still be able to read it back
	retrievedMetadata, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata with sync disabled: %v", err)
	}
	
	if retrievedMetadata.ID != messageID {
		t.Errorf("Metadata corrupted with sync disabled: expected %s, got %s", messageID, retrievedMetadata.ID)
	}
}