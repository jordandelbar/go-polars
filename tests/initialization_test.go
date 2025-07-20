package tests

import (
	"os"
	"testing"

	"github.com/jordandelbar/go-polars/polars"
)

// Test Initialization Functions
func TestInitializationFunctions(t *testing.T) {
	t.Run("IsInitialized", func(t *testing.T) {
		// Should be initialized since we use it in other tests
		if !polars.IsInitialized() {
			t.Error("Polars should be initialized")
		}
	})

	t.Run("GetInitError", func(t *testing.T) {
		// Should be nil if initialization was successful
		err := polars.GetInitError()
		if err != nil && polars.IsInitialized() {
			t.Errorf("Init error should be nil when initialized: %v", err)
		}
	})

	t.Run("ForceReinitialize", func(t *testing.T) {
		// Test reinitializing
		polars.ForceReinitialize()

		// Should still be initialized after reinitializing
		if !polars.IsInitialized() {
			t.Error("Polars should be initialized after ForceReinitialize")
		}

		// Error should be nil after successful reinitialization
		if polars.GetInitError() != nil {
			t.Error("Init error should be nil after successful reinitialization")
		}
	})
}

// Test Download Functions
func TestDownloadFunctions(t *testing.T) {
	t.Run("GetBinaryInfo", func(t *testing.T) {
		info, err := polars.GetBinaryInfo("")

		if err != nil {
			t.Errorf("GetBinaryInfo should not error: %v", err)
		}

		if info.LocalFilename == "" {
			t.Error("Binary filename should not be empty")
		}

		if info.URL == "" {
			t.Error("Binary URL should not be empty")
		}

		// Check that filename has appropriate extension
		expectedExts := []string{".so", ".dylib", ".dll"}
		hasValidExt := false
		for _, ext := range expectedExts {
			if len(info.LocalFilename) > len(ext) && info.LocalFilename[len(info.LocalFilename)-len(ext):] == ext {
				hasValidExt = true
				break
			}
		}
		if !hasValidExt {
			t.Errorf("Binary filename should have valid extension (.so, .dylib, .dll), got: %s", info.LocalFilename)
		}
	})

	t.Run("GetBinDirectory", func(t *testing.T) {
		binDir, err := polars.GetBinDirectory()

		if err != nil {
			t.Errorf("GetBinDirectory should not error: %v", err)
		}

		if binDir == "" {
			t.Error("Binary directory should not be empty")
		}

		// Check if directory exists or can be created
		if _, err := os.Stat(binDir); os.IsNotExist(err) {
			// Try to create it to test the path is valid
			if err := os.MkdirAll(binDir, 0755); err != nil {
				t.Errorf("Binary directory path should be valid: %v", err)
			} else {
				// Clean up test directory
				os.RemoveAll(binDir)
			}
		}
	})

	t.Run("CheckBinaryExists", func(t *testing.T) {
		exists, path, err := polars.CheckBinaryExists()

		if err != nil {
			t.Errorf("CheckBinaryExists should not error: %v", err)
		}

		// Since we're running tests successfully, binary should exist
		if !exists {
			t.Error("Binary should exist since tests are running")
		}

		if path == "" && exists {
			t.Error("Binary path should not be empty when binary exists")
		}
	})

	t.Run("EnsureBinary", func(t *testing.T) {
		// This should succeed since binary is already available
		err := polars.EnsureBinary()

		if err != nil {
			t.Errorf("EnsureBinary should succeed when binary is available: %v", err)
		}

		// Check binary exists after ensuring
		exists, _, err := polars.CheckBinaryExists()
		if err != nil {
			t.Errorf("CheckBinaryExists should not error: %v", err)
		}
		if !exists {
			t.Error("Binary should exist after EnsureBinary")
		}
	})

	t.Run("GetVersionFromEnv", func(t *testing.T) {
		// Test without environment variable - should return latest version
		version := polars.GetVersionFromEnv()
		if version == "" {
			t.Error("GetVersionFromEnv should return latest version when env var not set")
		}

		// Test with environment variable
		os.Setenv("GO_POLARS_VERSION", "test-version")
		defer os.Unsetenv("GO_POLARS_VERSION")

		version = polars.GetVersionFromEnv()
		if version != "test-version" {
			t.Errorf("GetVersionFromEnv should return env var value, got: %s", version)
		}
	})
}

// Test Download Edge Cases and Error Scenarios
func TestDownloadEdgeCases(t *testing.T) {
	t.Run("CleanOldBinaries", func(t *testing.T) {
		// Create a temporary test binary directory
		binDir, err := polars.GetBinDirectory()
		if err != nil {
			t.Skipf("Cannot get bin directory: %v", err)
		}
		testBinDir := binDir + "_test"
		if err := os.MkdirAll(testBinDir, 0755); err != nil {
			t.Skipf("Cannot create test directory: %v", err)
		}
		defer os.RemoveAll(testBinDir)

		// Create some fake old binary files
		oldFiles := []string{
			"libpolars_go_old.so",
			"libpolars_go_v1.0.0.so",
			"polars_go_old.dll",
		}

		for _, file := range oldFiles {
			f, err := os.Create(testBinDir + "/" + file)
			if err != nil {
				t.Skipf("Cannot create test file: %v", err)
			}
			f.Close()
		}

		// This function should be safe to call even if it doesn't find our test files
		polars.CleanOldBinaries()

		// The function should complete without error
		// (We can't easily test the actual cleaning without affecting the real binary)
	})

	t.Run("EnsureBinaryWhenMissing", func(t *testing.T) {
		// We can't easily test download without internet, but we can test the flow
		// The function should handle the case gracefully

		// Save current binary state
		originalExists, _, err := polars.CheckBinaryExists()
		if err != nil {
			t.Skipf("Cannot check binary exists: %v", err)
		}

		// Test that EnsureBinary handles the logic correctly
		err = polars.EnsureBinary()

		// Should either succeed or fail gracefully
		if err != nil {
			// If it fails, it should be for a legitimate reason
			t.Logf("EnsureBinary failed (expected in some test environments): %v", err)
		}

		// Binary existence should be consistent
		currentExists, _, err := polars.CheckBinaryExists()
		if err != nil {
			t.Logf("Error checking binary exists after EnsureBinary: %v", err)
		}
		if originalExists && !currentExists {
			t.Error("Binary should not disappear after EnsureBinary call")
		}
	})
}

// Test Integration Between Download and Initialization
func TestDownloadInitializationIntegration(t *testing.T) {
	t.Run("InitializationAfterEnsureBinary", func(t *testing.T) {
		// Ensure binary is available
		err := polars.EnsureBinary()
		if err != nil {
			t.Skipf("Cannot ensure binary for integration test: %v", err)
		}

		// Force reinitialization
		polars.ForceReinitialize()

		// Should be successfully initialized
		if !polars.IsInitialized() {
			t.Error("Should be initialized after ensuring binary and reinitializing")
		}

		// Should be able to perform basic operations
		df, err := polars.ReadCSV(getTestDataPath())
		if err != nil {
			t.Errorf("Should be able to read CSV after initialization: %v", err)
		}

		if df.Height() == 0 {
			t.Error("Should be able to get dataframe height after initialization")
		}
	})
}

// Test Error Handling in Download Functions
func TestDownloadErrorHandling(t *testing.T) {
	t.Run("HandleInvalidPaths", func(t *testing.T) {
		// Test that functions handle edge cases gracefully
		info, err := polars.GetBinaryInfo("")

		if err != nil {
			t.Errorf("GetBinaryInfo should not error: %v", err)
		}

		// These should return valid values even in edge cases
		if info.LocalFilename == "" || info.URL == "" {
			t.Error("GetBinaryInfo should always return non-empty values")
		}

		// GetBinDirectory should return a valid path
		binDir, err := polars.GetBinDirectory()
		if err != nil {
			t.Errorf("GetBinDirectory should not error: %v", err)
		}
		if binDir == "" {
			t.Error("GetBinDirectory should return a valid directory path")
		}
	})
}
