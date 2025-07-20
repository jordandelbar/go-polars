package polars

import (
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	initOnce sync.Once
	initErr  error
)

// init is called automatically when the package is imported
func init() {
	initOnce.Do(func() {
		initErr = initializePolars()
	})

	if initErr != nil {
		log.Printf("go-polars initialization failed: %v", initErr)
		log.Printf("You may need to run: go generate or build from source")
	}
}

// initializePolars performs runtime setup including binary download if needed
func initializePolars() error {
	// Check if we should skip download
	if os.Getenv("GO_POLARS_SKIP_DOWNLOAD") == "true" {
		// Still check if binary exists when skipping download
		exists, _, err := CheckBinaryExists()
		if err != nil {
			return fmt.Errorf("failed to check binary existence: %w", err)
		}
		if !exists {
			return fmt.Errorf("binary not found and download is disabled (GO_POLARS_SKIP_DOWNLOAD=true)")
		}
		return nil
	}

	// Ensure binary exists, downloading it if necessary
	if err := EnsureBinary(); err != nil {
		return fmt.Errorf("failed to ensure binary availability: %w", err)
	}

	return nil
}

// IsInitialized returns whether the library was successfully initialized
func IsInitialized() bool {
	return initErr == nil
}

// GetInitError returns the initialization error if any
func GetInitError() error {
	return initErr
}

// ForceReinitialize forces re-initialization (useful for testing or recovery)
func ForceReinitialize() error {
	initOnce = sync.Once{}
	initOnce.Do(func() {
		initErr = initializePolars()
	})
	return initErr
}
