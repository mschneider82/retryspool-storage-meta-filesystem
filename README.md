# Filesystem Metadata Storage

This package provides a robust, filesystem-based metadata storage backend for RetrySpool with atomic operations and crash recovery.

## Overview

This package provides a high-performance, crash-safe filesystem implementation of the RetrySpool metadata storage interface. It features atomic operations, configurable performance modes, asynchronous crash recovery, and optimized streaming batching for multi-worker environments.

## Installation

```bash
go get schneider.vip/retryspool/storage/meta/filesystem
```

## Usage

### Basic Usage (Standard Mode)
```go
import (
    metafs "schneider.vip/retryspool/storage/meta/filesystem"
    "schneider.vip/retryspool"
)

// Create filesystem metadata storage with full crash safety
factory := metafs.NewFactory("./metadata")

// Use with RetrySpool
queue := retryspool.New(
    retryspool.WithMetaStorage(factory),
    // ... other options
)
```

### Performance-Optimized Usage
```go
// High-performance mode (disable fsync for speed)
factory := metafs.NewFactoryWithPerformanceOptions("./metadata", true, false)

// Or using fluent API
factory := metafs.NewFactory("./metadata").WithDisableSync(true)

// Use with RetrySpool for loadtesting
queue := retryspool.New(
    retryspool.WithMetaStorage(factory),
    // ... other options
)
```

### Hybrid Mode (Balanced)
```go
// Balanced performance and safety
factory := metafs.NewFactory("./metadata").WithBatchSync(true)
```

### Direct Backend Usage
```go
// Store metadata
metadata := metastorage.MessageMetadata{
    ID:          "msg-123",
    State:       metastorage.StateIncoming,
    Attempts:    0,
    MaxAttempts: 3,
    Created:     time.Now(),
    Headers:     map[string]string{"to": "user@example.com"},
}

err = backend.StoreMeta(ctx, "msg-123", metadata)
if err != nil {
    panic(err)
}

// Atomic state transitions
err = backend.MoveToState(ctx, "msg-123", metastorage.StateIncoming, metastorage.StateActive)
if err != nil {
    panic(err)
}
```

## Directory Structure

The filesystem backend organizes metadata files by queue state with crash recovery support:

```
metadata/
├── .locks/              # Lock files for atomic operations
├── incoming/
│   ├── message1.json
│   ├── message2.json
│   └── .tmp.abc123      # Temp file (cleaned up on crash)
├── active/
│   └── message3.json
├── deferred/
├── hold/
└── bounce/
    └── message4.json
```

## Features

- **🔒 Atomic Operations**: Uses temp file + rename pattern for crash safety
- **🚀 Performance Modes**: Configurable sync options for speed vs safety trade-offs
- **🛡️ Asynchronous Crash Recovery**: Non-blocking cleanup of orphaned temp files on startup
- **📡 Streaming Batching**: Efficient directory streaming with constant memory usage
- **👥 Multi-Worker Optimized**: Thread-safe operations for concurrent worker environments
- **🔄 State-based Organization**: Messages organized by queue state directories
- **🔐 File Locking**: Prevents corruption during concurrent access
- **📁 Directory Sync**: Ensures rename operations are persistent
- **⚡ No External Dependencies**: Pure Go implementation using standard library
- **📊 Performance Monitoring**: Detailed logging and error reporting

## Performance Modes

| Mode | Fsync | Directory Sync | File Locking | Performance | Crash Safety |
|------|-------|----------------|--------------|-------------|--------------|
| **Standard** | ✅ | ✅ | ✅ | ~50 msg/sec | ⭐⭐⭐⭐⭐ |
| **Performance** | ❌ | ❌ | ⚠️ Optional | ~200+ msg/sec | ⭐⭐⭐ |
| **Hybrid** | ⚠️ Batch | ⚠️ Batch | ✅ | ~120 msg/sec | ⭐⭐⭐⭐ |

## Metadata File Format

Each metadata file is stored as JSON:

```json
{
  "ID": "msg-123",
  "State": 0,
  "Attempts": 1,
  "MaxAttempts": 3,
  "NextRetry": "2023-01-01T12:00:00Z",
  "Created": "2023-01-01T10:00:00Z",
  "Updated": "2023-01-01T10:30:00Z",
  "LastError": "temporary failure",
  "Size": 1024,
  "Priority": 5,
  "Headers": {
    "to": "user@example.com",
    "from": "sender@example.com"
  }
}
```

## Robustness Features

### Atomic State Transitions
```go
// Atomic file-based state verification
err := backend.MoveToState(ctx, messageID, fromState, toState)
// - Verifies file exists in expected state
// - Uses file locking to prevent race conditions
// - Atomic temp file + rename pattern
```

### Crash Recovery
```go
// Automatic cleanup on startup
backend, err := metafs.NewBackend("./metadata")
// - Scans for orphaned .tmp files
// - Removes incomplete operations
// - Logs cleanup actions
```

### Crash Safety Features
- **Temp File Pattern**: Write to `.tmp.{random}` then atomic rename
- **Directory Sync**: Ensures rename operations survive power loss
- **File Locking**: Prevents concurrent corruption
- **Fsync Before Rename**: Forces data to disk before atomic operations

## Configuration Options

### Factory Options
```go
// Standard factory (safe, slower)
factory := metafs.NewFactory("./metadata")

// Performance factory (fast, less safe)
factory := metafs.NewFactoryWithPerformanceOptions("./metadata", true, false)

// Fluent configuration
factory := metafs.NewFactory("./metadata").
    WithDisableSync(true).     // Disable fsync for speed
    WithBatchSync(false)       // Disable batch sync
```

### Backend Options
```go
// Direct backend creation with options
backend, err := metafs.NewBackendWithOptions("./metadata", disableSync, batchSync)
```

## Performance Tuning

### For Loadtesting (Maximum Speed)
```go
factory := metafs.NewFactory("./data").WithDisableSync(true)
// Expected: ~200+ msg/sec
// Trade-off: Less crash safety
```

### For Production (Maximum Safety)
```go
factory := metafs.NewFactory("./data") // Default: all safety features enabled
// Expected: ~50 msg/sec
// Trade-off: Slower but crash-safe
```

### For Development (Balanced)
```go
factory := metafs.NewFactory("./data").WithBatchSync(true)
// Expected: ~120 msg/sec
// Trade-off: Good balance of speed and safety
```

## File Permissions

- Directories: `0755` (rwxr-xr-x)
- Files: Default OS permissions (usually `0644`)

## Error Handling

- Returns `ErrMessageNotFound` for missing messages
- Returns `ErrBackendClosed` when backend is closed
- Handles filesystem errors gracefully
- Provides detailed error messages with context

## State Transitions

State transitions are atomic:
1. Create new metadata file in target state directory
2. Remove old metadata file from source state directory
3. If any step fails, operation is rolled back

## Error Handling

The filesystem backend provides detailed error reporting:

```go
err := backend.MoveToState(ctx, messageID, fromState, toState)
if err != nil {
    // Specific error types:
    // - "message not found in state X"
    // - "message is in state Y, expected X"
    // - "failed to acquire message lock"
}
```

## Limitations

- **Single-node only**: Not designed for distributed scenarios
- **File system dependent**: Performance varies by filesystem type
- **Directory scanning overhead**: Listing operations scan directories
- **Platform-specific locking**: File locking behavior may vary

## Best Use Cases

### ✅ Recommended For:
- **Development and testing**: No external dependencies
- **Small to medium workloads**: < 1000 msg/sec
- **Single-node deployments**: Local file storage
- **Crash-sensitive applications**: When data loss is unacceptable

### ❌ Not Recommended For:
- **High-throughput scenarios**: > 1000 msg/sec sustained
- **Distributed deployments**: Multiple nodes accessing same storage
- **Network file systems**: NFS, CIFS may have locking issues
- **Very large queues**: > 100k messages per state

## Comparison with Other Backends

| Feature | Filesystem | SQLite | PostgreSQL | NATS |
|---------|------------|--------|------------|------|
| **Setup Complexity** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| **Performance** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Crash Safety** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Atomic Operations** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Scalability** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

**The filesystem backend is now a robust alternative to SQLite for single-node deployments!**