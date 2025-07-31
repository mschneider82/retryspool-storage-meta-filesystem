package filesystem

import (
	metastorage "schneider.vip/retryspool/storage/meta"
)

// Factory implements metastorage.Factory for filesystem storage
type Factory struct {
	basePath    string
	disableSync bool // Performance optimization: disable fsync operations
	batchSync   bool // Performance optimization: batch sync operations
}

// NewFactory creates a new filesystem metadata storage factory
func NewFactory(basePath string) *Factory {
	return &Factory{
		basePath:    basePath,
		disableSync: false,
		batchSync:   false,
	}
}

// NewFactoryWithPerformanceOptions creates a factory with performance tuning options
func NewFactoryWithPerformanceOptions(basePath string, disableSync bool, batchSync bool) *Factory {
	return &Factory{
		basePath:    basePath,
		disableSync: disableSync,
		batchSync:   batchSync,
	}
}

// WithDisableSync disables fsync operations for better performance (less crash safety)
func (f *Factory) WithDisableSync(disable bool) *Factory {
	f.disableSync = disable
	return f
}

// WithBatchSync enables batched sync operations for better performance
func (f *Factory) WithBatchSync(enable bool) *Factory {
	f.batchSync = enable
	return f
}

// Create creates a new filesystem metadata storage backend
func (f *Factory) Create() (metastorage.Backend, error) {
	return NewBackendWithOptions(f.basePath, f.disableSync, f.batchSync)
}

// Name returns the factory name
func (f *Factory) Name() string {
	return "filesystem-meta"
}