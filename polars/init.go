package polars

import (
	"fmt"
	"log"
	"sync"
)

var (
	initOnce sync.Once
	initErr  error
)

// init is called automatically when the package is imported
// Binary setup is now handled by go:generate before compilation
func init() {
	initOnce.Do(func() {
		initErr = initializePolars()
	})

	if initErr != nil {
		log.Printf("go-polars initialization failed: %v", initErr)
		log.Printf("Binary should have been set up by go:generate. If this fails, please run: go generate")
	}
}

// initializePolars performs minimal runtime checks since binary setup is done at build time
func initializePolars() error {
	// Quick sanity check that binary exists (it should already be there from go:generate)
	exists, _, err := CheckBinaryExists()
	if err != nil {
		return fmt.Errorf("failed to check binary existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("binary not found - go:generate should have downloaded it. Try running: go generate")
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
