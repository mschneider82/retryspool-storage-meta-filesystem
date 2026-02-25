package filesystem

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metastorage "schneider.vip/retryspool/storage/meta"
)

// messageLock manages per-message read/write locking
type messageLock struct {
	mu       sync.RWMutex
	refCount int32 // atomic: wie viele Goroutinen halten/wollen diesen Lock
}

// Backend implements metastorage.Backend for filesystem storage
type Backend struct {
	basePath string

	// Per-Message Locks für CAS-Atomizität
	messageLocks sync.Map // map[string]*messageLock

	// Nur für closed-Flag
	closedMu sync.RWMutex
	closed   bool

	// Performance optimizations
	disableSync bool // Disable fsync for better performance
	batchSync   bool // Batch sync operations
}

// getMessageLock gibt den Lock für eine Message zurück (erstellt ihn bei Bedarf)
func (b *Backend) getMessageLock(messageID string) *messageLock {
	val, _ := b.messageLocks.LoadOrStore(messageID, &messageLock{})
	lock := val.(*messageLock)
	atomic.AddInt32(&lock.refCount, 1)
	return lock
}

// releaseMessageLock gibt den Lock frei und räumt auf wenn keiner ihn mehr braucht
func (b *Backend) releaseMessageLock(messageID string, lock *messageLock) {
	if atomic.AddInt32(&lock.refCount, -1) <= 0 {
		b.messageLocks.CompareAndDelete(messageID, lock)
	}
}

// NewBackend creates a new filesystem metadata storage backend
func NewBackend(basePath string) (*Backend, error) {
	return NewBackendWithOptions(basePath, false, false)
}

// NewBackendWithOptions creates a filesystem backend with performance options
func NewBackendWithOptions(basePath string, disableSync bool, batchSync bool) (*Backend, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// Create queue state directories
	for _, state := range []metastorage.QueueState{
		metastorage.StateIncoming,
		metastorage.StateActive,
		metastorage.StateDeferred,
		metastorage.StateHold,
		metastorage.StateBounce,
	} {
		dir := filepath.Join(basePath, state.String())
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create state directory %s: %w", dir, err)
		}
	}

	backend := &Backend{
		basePath:    basePath,
		disableSync: disableSync,
		batchSync:   batchSync,
	}

	// Perform crash recovery on startup
	if err := backend.RecoverFromCrash(); err != nil {
		log.Printf("Warning: crash recovery failed: %v", err)
	}

	return backend, nil
}

// StoreMeta stores message metadata
func (b *Backend) StoreMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	lock := b.getMessageLock(messageID)
	lock.mu.Lock()
	defer func() {
		lock.mu.Unlock()
		b.releaseMessageLock(messageID, lock)
	}()

	// Update metadata timestamps
	metadata.Updated = time.Now()
	if metadata.Created.IsZero() {
		metadata.Created = metadata.Updated
	}

	metaPath := b.getMetaPath(messageID, metadata.State)

	// Create directory if needed
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Store metadata as JSON atomically
	if err := b.storeMetadataFileAtomic(metaPath, metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	return nil
}

// GetMeta retrieves message metadata
func (b *Backend) GetMeta(ctx context.Context, messageID string) (metastorage.MessageMetadata, error) {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.MessageMetadata{}, metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	lock := b.getMessageLock(messageID)
	lock.mu.RLock()
	defer func() {
		lock.mu.RUnlock()
		b.releaseMessageLock(messageID, lock)
	}()

	return b.getMetadata(messageID)
}

// UpdateMeta updates message metadata
func (b *Backend) UpdateMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	lock := b.getMessageLock(messageID)
	lock.mu.Lock()
	defer func() {
		lock.mu.Unlock()
		b.releaseMessageLock(messageID, lock)
	}()

	// Get current metadata to preserve state location
	currentMeta, err := b.getMetadata(messageID)
	if err != nil {
		return err
	}

	// Update timestamp
	metadata.Updated = time.Now()
	metadata.Created = currentMeta.Created // Preserve original creation time

	// If state changed, move the file atomically
	if metadata.State != currentMeta.State {
		return b.moveMetadataAtomic(messageID, currentMeta.State, metadata.State, metadata)
	}

	// Same state, just update the file atomically
	metaPath := b.getMetaPath(messageID, metadata.State)
	return b.storeMetadataFileAtomic(metaPath, metadata)
}

// DeleteMeta removes message metadata
func (b *Backend) DeleteMeta(ctx context.Context, messageID string) error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	lock := b.getMessageLock(messageID)
	lock.mu.Lock()
	defer func() {
		lock.mu.Unlock()
		b.releaseMessageLock(messageID, lock)
	}()

	// Find and remove metadata file from any state
	for _, state := range []metastorage.QueueState{
		metastorage.StateIncoming,
		metastorage.StateActive,
		metastorage.StateDeferred,
		metastorage.StateHold,
		metastorage.StateBounce,
	} {
		metaPath := b.getMetaPath(messageID, state)
		if err := os.Remove(metaPath); err == nil {
			// Sync directory to ensure deletion is persistent
			if !b.disableSync {
				if syncErr := b.syncDirectory(filepath.Dir(metaPath)); syncErr != nil {
					log.Printf("Warning: failed to sync directory %s after metadata deletion: %v", filepath.Dir(metaPath), syncErr)
				}
			}
			return nil // Successfully removed
		}
	}

	return metastorage.ErrMessageNotFound
}

// MoveToState moves a message from one queue state to another atomically.
// The storage layer guarantees CAS semantics — no read-check-write needed.
func (b *Backend) MoveToState(ctx context.Context, messageID string, fromState, toState metastorage.QueueState) error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	if fromState == toState {
		return nil
	}

	// Per-Message EXCLUSIVE Lock → das ist die CAS-Garantie!
	// Nur ein Caller pro Message kommt hier gleichzeitig rein.
	lock := b.getMessageLock(messageID)
	lock.mu.Lock()
	defer func() {
		lock.mu.Unlock()
		b.releaseMessageLock(messageID, lock)
	}()

	// Unter dem Lock: Prüfe ob Datei im erwarteten State existiert
	oldPath := b.getMetaPath(messageID, fromState)
	if _, err := os.Stat(oldPath); os.IsNotExist(err) {
		return metastorage.ErrStateConflict // ← Definierter Sentinel Error
	}

	// Lade und verifiziere (Belt & Suspenders)
	metadata, err := b.loadMetadataFile(oldPath)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}
	if metadata.State != fromState {
		return metastorage.ErrStateConflict
	}

	// State-Transition
	metadata.State = toState
	metadata.Updated = time.Now()

	return b.moveMetadataAtomic(messageID, fromState, toState, metadata)
}

// ListMessages lists messages with pagination and filtering
func (b *Backend) ListMessages(ctx context.Context, state metastorage.QueueState, options metastorage.MessageListOptions) (metastorage.MessageListResult, error) {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.MessageListResult{}, metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	stateDir := filepath.Join(b.basePath, state.String())
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return metastorage.MessageListResult{MessageIDs: []string{}, Total: 0, HasMore: false}, nil
		}
		return metastorage.MessageListResult{}, fmt.Errorf("failed to read state directory: %w", err)
	}

	// Collect message info for sorting and filtering
	type messageInfo struct {
		id       string
		metadata metastorage.MessageMetadata
	}

	var messages []messageInfo
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		messageID := strings.TrimSuffix(entry.Name(), ".json")
		metaPath := filepath.Join(stateDir, entry.Name())

		metadata, err := b.loadMetadataFile(metaPath)
		if err != nil {
			continue // Skip invalid metadata files
		}

		// Apply time filter
		if !options.Since.IsZero() {
			if metadata.Created.Before(options.Since) && metadata.Updated.Before(options.Since) {
				continue
			}
		}

		messages = append(messages, messageInfo{
			id:       messageID,
			metadata: metadata,
		})
	}

	// Sort messages
	if options.SortBy != "" {
		sort.Slice(messages, func(i, j int) bool {
			var less bool
			switch options.SortBy {
			case "created":
				less = messages[i].metadata.Created.Before(messages[j].metadata.Created)
			case "updated":
				less = messages[i].metadata.Updated.Before(messages[j].metadata.Updated)
			case "priority":
				less = messages[i].metadata.Priority < messages[j].metadata.Priority
			case "attempts":
				less = messages[i].metadata.Attempts < messages[j].metadata.Attempts
			default:
				less = messages[i].metadata.Created.Before(messages[j].metadata.Created)
			}

			if options.SortOrder == "desc" {
				return !less
			}
			return less
		})
	}

	// Apply pagination
	total := len(messages)
	start := options.Offset
	if start > total {
		start = total
	}

	end := start + options.Limit
	if options.Limit == 0 || end > total {
		end = total
	}

	hasMore := end < total
	messageIDs := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		messageIDs = append(messageIDs, messages[i].id)
	}

	return metastorage.MessageListResult{
		MessageIDs: messageIDs,
		Total:      total,
		HasMore:    hasMore,
	}, nil
}

// NewMessageIterator creates an iterator for messages in a specific state
func (b *Backend) NewMessageIterator(ctx context.Context, state metastorage.QueueState, batchSize int) (metastorage.MessageIterator, error) {
	// Check if backend is closed with proper synchronization
	b.closedMu.RLock()
	closed := b.closed
	b.closedMu.RUnlock()

	if closed {
		return nil, metastorage.ErrBackendClosed
	}

	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	return &filesystemIterator{
		backend:   b,
		state:     state,
		batchSize: batchSize,
		ctx:       ctx,
	}, nil
}

// filesystemIterator implements MessageIterator for filesystem backend
type filesystemIterator struct {
	backend   *Backend
	state     metastorage.QueueState
	batchSize int
	ctx       context.Context

	// Current state
	entries  []os.DirEntry
	current  int
	offset   int
	stateDir string
	closed   bool
	mu       sync.RWMutex
}

// Next returns the next message metadata
func (it *filesystemIterator) Next(ctx context.Context) (metastorage.MessageMetadata, bool, error) {
	// Check context cancellation first
	select {
	case <-ctx.Done():
		return metastorage.MessageMetadata{}, false, ctx.Err()
	default:
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("iterator is closed")
	}

	// Initialize on first call
	if it.entries == nil {
		if err := it.loadBatch(); err != nil {
			return metastorage.MessageMetadata{}, false, err
		}
	}

	// Loop to find next valid entry (avoid recursion)
	for {
		// Check context cancellation in loop
		select {
		case <-ctx.Done():
			return metastorage.MessageMetadata{}, false, ctx.Err()
		default:
		}

		// Check if we need to load next batch
		if it.current >= len(it.entries) {
			if err := it.loadBatch(); err != nil {
				return metastorage.MessageMetadata{}, false, err
			}
			// If still no entries, we're done
			if len(it.entries) == 0 {
				return metastorage.MessageMetadata{}, false, nil
			}
		}

		// Process current entry
		entry := it.entries[it.current]
		it.current++

		if !strings.HasSuffix(entry.Name(), ".json") {
			// Skip non-JSON files and try next (continue loop)
			continue
		}

		messageID := strings.TrimSuffix(entry.Name(), ".json")
		metaPath := filepath.Join(it.stateDir, entry.Name())

		// CRITICAL FIX: Acquire backend lock during metadata loading to prevent race conditions
		// Note: Iterator uses per-message read lock for consistency during metadata load
		lock := it.backend.getMessageLock(messageID)
		lock.mu.RLock()
		metadata, err := it.backend.loadMetadataFile(metaPath)
		lock.mu.RUnlock()
		it.backend.releaseMessageLock(messageID, lock)

		if err != nil {
			// Skip invalid metadata files and try next (continue loop)
			continue
		}

		// Set the message ID
		metadata.ID = messageID

		// Check if there are more entries
		hasMore := it.current < len(it.entries) || len(it.entries) == it.batchSize

		return metadata, hasMore, nil
	}
}

// EntryInfo holds information about a directory entry to process
type EntryInfo struct {
	Entry     os.DirEntry
	StateDir  string
	Current   int
	BatchSize int
}

// initializeBatch initializes the iterator on first use
func (it *filesystemIterator) initializeBatch(ctx context.Context) error {
	return it.loadNextBatch(ctx)
}

// getNextEntryInfo gets the next entry to process with minimal lock time
func (it *filesystemIterator) getNextEntryInfo() (EntryInfo, bool, bool) {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Check if we need to load next batch
	if it.current >= len(it.entries) {
		return EntryInfo{}, true, false // needsNewBatch = true
	}

	// Check if we're done
	if len(it.entries) == 0 {
		return EntryInfo{}, false, true // done = true
	}

	// Ensure stateDir is set
	if it.stateDir == "" {
		it.stateDir = filepath.Join(it.backend.basePath, it.state.String())
	}

	// Get current entry and advance
	entry := it.entries[it.current]
	current := it.current
	it.current++

	return EntryInfo{
		Entry:     entry,
		StateDir:  it.stateDir,
		Current:   current,
		BatchSize: it.batchSize,
	}, false, false
}

// loadNextBatch loads the next batch of entries
func (it *filesystemIterator) loadNextBatch(ctx context.Context) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Load batch with backend closed check
	it.backend.closedMu.RLock()
	if it.backend.closed {
		it.backend.closedMu.RUnlock()
		return metastorage.ErrBackendClosed
	}
	it.backend.closedMu.RUnlock()

	entries, err := it.loadBatchFromFilesystem()

	if err != nil {
		return err
	}

	// Update iterator state with minimal lock time
	it.mu.Lock()
	it.entries = entries
	it.current = 0
	it.offset += len(entries)
	it.mu.Unlock()

	return nil
}

// loadBatchFromFilesystem loads entries from filesystem (assumes backend lock is held)
func (it *filesystemIterator) loadBatchFromFilesystem() ([]os.DirEntry, error) {
	// Ensure stateDir is set (read from iterator state safely)
	it.mu.RLock()
	stateDir := it.stateDir
	if stateDir == "" {
		stateDir = filepath.Join(it.backend.basePath, it.state.String())
		// Update the iterator's stateDir
		it.mu.RUnlock()
		it.mu.Lock()
		it.stateDir = stateDir
		it.mu.Unlock()
	} else {
		it.mu.RUnlock()
	}

	// Open directory for streaming reads
	dir, err := os.Open(it.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []os.DirEntry{}, nil
		}
		return nil, fmt.Errorf("failed to open state directory: %w", err)
	}
	defer dir.Close()

	// Skip entries we've already processed
	if it.offset > 0 {
		for skipped := 0; skipped < it.offset; {
			batch, err := dir.ReadDir(100)
			if err != nil || len(batch) == 0 {
				return []os.DirEntry{}, nil
			}
			skipped += len(batch)
			if skipped > it.offset {
				excess := skipped - it.offset
				return batch[len(batch)-excess:], nil
			}
		}
	}

	// Read the actual batch we want
	entries, err := dir.ReadDir(it.batchSize)
	if err != nil && len(entries) == 0 {
		return []os.DirEntry{}, nil
	}

	return entries, nil
}

// processEntry processes a single directory entry
func (it *filesystemIterator) processEntry(ctx context.Context, entryInfo EntryInfo) (metastorage.MessageMetadata, bool, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return metastorage.MessageMetadata{}, false, ctx.Err()
	default:
	}

	// Validate entry info
	if entryInfo.Entry == nil {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("invalid entry info: nil entry")
	}

	// Skip non-JSON files
	if !strings.HasSuffix(entryInfo.Entry.Name(), ".json") {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("skip non-json file")
	}

	messageID := strings.TrimSuffix(entryInfo.Entry.Name(), ".json")

	// Ensure we have a valid state directory
	stateDir := entryInfo.StateDir
	if stateDir == "" {
		it.mu.RLock()
		if it.stateDir == "" {
			it.stateDir = filepath.Join(it.backend.basePath, it.state.String())
		}
		stateDir = it.stateDir
		it.mu.RUnlock()
	}

	metaPath := filepath.Join(stateDir, entryInfo.Entry.Name())

	// Load metadata with backend per-message lock (no iterator lock needed)
	lock := it.backend.getMessageLock(messageID)
	lock.mu.RLock()
	metadata, err := it.backend.loadMetadataFile(metaPath)
	lock.mu.RUnlock()
	it.backend.releaseMessageLock(messageID, lock)

	if err != nil {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("failed to load metadata: %w", err)
	}

	// Set the message ID
	metadata.ID = messageID

	// Check if there are more entries (read current state safely)
	it.mu.RLock()
	hasMore := it.current < len(it.entries) || len(it.entries) == entryInfo.BatchSize
	it.mu.RUnlock()

	return metadata, hasMore, nil
}

// loadBatch loads the next batch of directory entries with proper streaming
func (it *filesystemIterator) loadBatch() error {
	if it.stateDir == "" {
		it.stateDir = filepath.Join(it.backend.basePath, it.state.String())
	}

	// CRITICAL FIX: Acquire backend closedMu during directory reading to prevent race conditions
	it.backend.closedMu.RLock()
	defer it.backend.closedMu.RUnlock()

	// Open directory for streaming reads
	dir, err := os.Open(it.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			it.entries = []os.DirEntry{}
			return nil
		}
		return fmt.Errorf("failed to open state directory: %w", err)
	}
	defer dir.Close()

	// Skip entries we've already processed
	if it.offset > 0 {
		// Read and discard entries up to our offset
		for skipped := 0; skipped < it.offset; {
			batch, err := dir.ReadDir(100) // Read in chunks to avoid memory issues
			if err != nil || len(batch) == 0 {
				// No more entries or error - we're done
				it.entries = []os.DirEntry{}
				return nil
			}
			skipped += len(batch)
			if skipped > it.offset {
				// We read too many, keep the excess for this batch
				excess := skipped - it.offset
				it.entries = batch[len(batch)-excess:]
				it.current = 0
				it.offset = skipped
				return nil
			}
		}
	}

	// Read the actual batch we want
	entries, err := dir.ReadDir(it.batchSize)
	if err != nil && len(entries) == 0 {
		// End of directory or error
		it.entries = []os.DirEntry{}
		return nil
	}

	it.entries = entries
	it.current = 0
	it.offset += len(entries)

	return nil
}

// Close closes the iterator
func (it *filesystemIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.closed = true
	it.entries = nil
	return nil
}

// Close closes the metadata storage backend
func (b *Backend) Close() error {
	b.closedMu.Lock()
	defer b.closedMu.Unlock()

	b.closed = true
	return nil
}

// Helper methods

func (b *Backend) getMetaPath(messageID string, state metastorage.QueueState) string {
	return filepath.Join(b.basePath, state.String(), messageID+".json")
}

func (b *Backend) getMetadata(messageID string) (metastorage.MessageMetadata, error) {
	// Try to find the message in any state
	for _, state := range []metastorage.QueueState{
		metastorage.StateIncoming,
		metastorage.StateActive,
		metastorage.StateDeferred,
		metastorage.StateHold,
		metastorage.StateBounce,
	} {
		metaPath := b.getMetaPath(messageID, state)
		if _, err := os.Stat(metaPath); err == nil {
			return b.loadMetadataFile(metaPath)
		}
	}

	return metastorage.MessageMetadata{}, metastorage.ErrMessageNotFound
}

func (b *Backend) moveMetadataAtomic(messageID string, oldState, newState metastorage.QueueState, metadata metastorage.MessageMetadata) error {
	oldPath := b.getMetaPath(messageID, oldState)
	newPath := b.getMetaPath(messageID, newState)

	// Create new directory if needed
	newDir := filepath.Dir(newPath)
	if err := os.MkdirAll(newDir, 0o755); err != nil {
		return fmt.Errorf("failed to create new state directory: %w", err)
	}

	// Create unique temp file to avoid conflicts
	tempPath := newPath + ".tmp." + b.generateRandomSuffix()
	if err := b.storeMetadataFile(tempPath, metadata); err != nil {
		return fmt.Errorf("failed to store metadata in temp file: %w", err)
	}

	// Atomic move: rename temp file to final location
	if err := os.Rename(tempPath, newPath); err != nil {
		os.Remove(tempPath) // Clean up temp file on failure
		return fmt.Errorf("failed to rename temp file to final location: %w", err)
	}

	// Sync directory to ensure rename is persistent - optional for performance
	if !b.disableSync {
		if err := b.syncDirectory(newDir); err != nil {
			log.Printf("Warning: failed to sync new directory %s: %v", newDir, err)
		}
	}

	// Remove old metadata file (only after successful atomic move)
	if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
		// This is less critical - the new file is already in place
		log.Printf("Warning: failed to remove old metadata %s (new file is in place): %v", oldPath, err)
	}

	// Sync old directory too - optional for performance
	if !b.disableSync {
		if err := b.syncDirectory(filepath.Dir(oldPath)); err != nil {
			log.Printf("Warning: failed to sync old directory: %v", err)
		}
	}

	return nil
}

func (b *Backend) storeMetadataFile(path string, metadata metastorage.MessageMetadata) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(metadata)
}

// storeMetadataFileAtomic stores metadata atomically using temp file + rename with crash safety
func (b *Backend) storeMetadataFileAtomic(path string, metadata metastorage.MessageMetadata) error {
	// Create unique temporary file in the same directory
	tempPath := path + ".tmp." + b.generateRandomSuffix()

	// Create temp file
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Ensure cleanup on failure
	defer func() {
		tempFile.Close()
		if err != nil {
			os.Remove(tempPath)
		}
	}()

	// Write JSON data
	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	// Force write to disk before rename (crash safety) - optional for performance
	if !b.disableSync {
		if err = tempFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync temp file: %w", err)
		}
	}

	tempFile.Close()

	// Atomic rename to final location
	if err = os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Sync directory to ensure rename is persistent - optional for performance
	if !b.disableSync {
		if err = b.syncDirectory(filepath.Dir(path)); err != nil {
			log.Printf("Warning: failed to sync directory: %v", err)
		}
	}

	return nil
}

func (b *Backend) loadMetadataFile(path string) (metastorage.MessageMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return metastorage.MessageMetadata{}, err
	}
	defer file.Close()

	var metadata metastorage.MessageMetadata
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&metadata)
	return metadata, err
}

// loadMetadataFileWithLock loads metadata with optional file locking for concurrent safety
func (b *Backend) loadMetadataFileWithLock(path string) (metastorage.MessageMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return metastorage.MessageMetadata{}, err
	}
	defer file.Close()

	// Acquire shared lock for reading - skip for performance if not needed
	if !b.disableSync {
		if err := b.lockFile(file, false); err != nil {
			// If locking fails, continue without lock for performance
			log.Printf("Warning: failed to lock file %s, continuing without lock: %v", path, err)
		} else {
			defer b.unlockFile(file)
		}
	}

	var metadata metastorage.MessageMetadata
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&metadata)
	return metadata, err
}

// RecoverFromCrash performs crash recovery by cleaning up orphaned temp files
func (b *Backend) RecoverFromCrash() error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return metastorage.ErrBackendClosed
	}
	b.closedMu.RUnlock()

	log.Println("Starting filesystem crash recovery...")

	tempFileCount := 0
	err := filepath.Walk(b.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Continue walking
		}

		// Clean up orphaned temp files
		if strings.Contains(info.Name(), ".tmp.") || strings.HasSuffix(info.Name(), ".tmp") {
			log.Printf("Removing orphaned temp file: %s", path)
			os.Remove(path)
			tempFileCount++
		}

		return nil
	})

	if tempFileCount > 0 {
		log.Printf("Cleaned up %d orphaned temp files", tempFileCount)
	}

	return err
}

// syncDirectory forces directory metadata to disk for crash safety
func (b *Backend) syncDirectory(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer dir.Close()

	return dir.Sync()
}

// generateRandomSuffix creates a unique suffix for temp files
func (b *Backend) generateRandomSuffix() string {
	randBytes := make([]byte, 4)
	rand.Read(randBytes)
	return hex.EncodeToString(randBytes)
}

// lockFile applies file locking for concurrent access protection
func (b *Backend) lockFile(file *os.File, exclusive bool) error {
	lockType := syscall.LOCK_SH // Shared lock
	if exclusive {
		lockType = syscall.LOCK_EX // Exclusive lock
	}

	return syscall.Flock(int(file.Fd()), lockType|syscall.LOCK_NB)
}

// unlockFile removes file lock
func (b *Backend) unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

// asyncCleanupTempFiles performs background cleanup of orphaned temp files
func (b *Backend) asyncCleanupTempFiles() {
	log.Println("Background temp file cleanup started...")

	tempFileCount := 0
	startTime := time.Now()

	err := filepath.Walk(b.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Clean up orphaned temp files
		if strings.Contains(info.Name(), ".tmp.") || strings.HasSuffix(info.Name(), ".tmp") {
			// Individual os.Remove() for atomicity - prevents double processing
			if removeErr := os.Remove(path); removeErr != nil {
				log.Printf("Warning: failed to remove temp file %s: %v", path, removeErr)
			} else {
				tempFileCount++

				// Rate limiting: small delay every 100 files to avoid I/O overload
				if tempFileCount%100 == 0 {
					time.Sleep(10 * time.Millisecond)
					log.Printf("Background cleanup progress: %d temp files removed", tempFileCount)
				}
			}
		}

		return nil
	})

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("Background crash recovery failed: %v", err)
		return
	}

	if tempFileCount > 0 {
		log.Printf("Background crash recovery completed: removed %d orphaned temp files in %v", tempFileCount, duration)
	} else {
		log.Printf("Background crash recovery completed: no orphaned temp files found (took %v)", duration)
	}
}
