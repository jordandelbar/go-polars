package polars

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	// GitHub release URL pattern
	releaseURLPattern = "https://github.com/jordandelbar/go-polars/releases/download/%s/%s"

	// GitHub API URL for latest release
	latestReleaseAPI = "https://api.github.com/repos/jordandelbar/go-polars/releases/latest"

	// Fallback version if API fails
	fallbackVersion = "v0.0.16"

	// Binary filenames for different platforms
	linuxBinary   = "libpolars_go-linux-amd64-%s.so"
	darwinBinary  = "libpolars_go-darwin-amd64-%s.dylib"
	windowsBinary = "polars_go-windows-amd64-%s.dll"

	// Expected local filenames after download
	linuxLocalName   = "libpolars_go.so"
	darwinLocalName  = "libpolars_go.dylib"
	windowsLocalName = "polars_go.dll"
)

// BinaryInfo contains information about a platform binary
type BinaryInfo struct {
	URL           string
	LocalFilename string
	SHA256        string
}

// GitHubRelease represents a GitHub release response
type GitHubRelease struct {
	TagName string `json:"tag_name"`
}

// getLatestVersion fetches the latest release version from GitHub API
func getLatestVersion() string {
	// Check if version is overridden by environment variable
	if version := os.Getenv("GO_POLARS_VERSION"); version != "" {
		return version
	}

	// Try to fetch latest version from GitHub API
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(latestReleaseAPI)
	if err != nil {
		return fallbackVersion
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fallbackVersion
	}

	var release GitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return fallbackVersion
	}

	if release.TagName == "" {
		return fallbackVersion
	}

	return release.TagName
}

// GetBinaryInfo returns the binary information for the current platform
func GetBinaryInfo(version string) (*BinaryInfo, error) {
	if version == "" {
		version = getLatestVersion()
	}

	var remoteName, localName string

	switch runtime.GOOS {
	case "linux":
		remoteName = fmt.Sprintf(linuxBinary, version)
		localName = linuxLocalName
	case "darwin":
		remoteName = fmt.Sprintf(darwinBinary, version)
		localName = darwinLocalName
	case "windows":
		remoteName = fmt.Sprintf(windowsBinary, version)
		localName = windowsLocalName
	default:
		return nil, fmt.Errorf("unsupported platform: %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	url := fmt.Sprintf(releaseURLPattern, version, remoteName)

	return &BinaryInfo{
		URL:           url,
		LocalFilename: localName,
	}, nil
}

// GetBinDirectory returns the path to the standard cache directory for binaries
func GetBinDirectory() (string, error) {
	var binDir string

	// Use /tmp/go-polars for simplicity and universal compatibility
	switch runtime.GOOS {
	case "windows":
		tempDir := os.TempDir()
		binDir = filepath.Join(tempDir, "go-polars")
	default: // Linux, macOS and other Unix-like systems
		binDir = "/tmp/go-polars"
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create bin directory: %w", err)
	}

	return binDir, nil
}

// CheckBinaryExists checks if the required binary exists locally
func CheckBinaryExists() (bool, string, error) {
	binDir, err := GetBinDirectory()
	if err != nil {
		return false, "", err
	}

	info, err := GetBinaryInfo("")
	if err != nil {
		return false, "", err
	}

	binaryPath := filepath.Join(binDir, info.LocalFilename)

	if _, err := os.Stat(binaryPath); err == nil {
		return true, binaryPath, nil
	}

	return false, binaryPath, nil
}

// DownloadBinary downloads the binary for the current platform
func DownloadBinary(version string) error {
	info, err := GetBinaryInfo(version)
	if err != nil {
		return fmt.Errorf("failed to get binary info: %w", err)
	}

	binDir, err := GetBinDirectory()
	if err != nil {
		return fmt.Errorf("failed to get bin directory: %w", err)
	}

	binaryPath := filepath.Join(binDir, info.LocalFilename)

	fmt.Printf("Downloading go-polars binary for %s/%s...\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("URL: %s\n", info.URL)

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}

	// Download the binary
	resp, err := client.Get(info.URL)
	if err != nil {
		return fmt.Errorf("failed to download binary: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("binary not available for download - you may need to build from source using: ./build.sh")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download binary: HTTP %d", resp.StatusCode)
	}

	// Create temporary file
	tempFile, err := os.CreateTemp(binDir, "libpolars_go_*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Copy with progress indication for large files
	hasher := sha256.New()
	writer := io.MultiWriter(tempFile, hasher)

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to save binary: %w", err)
	}

	tempFile.Close()

	// Verify checksum if available
	if info.SHA256 != "" {
		actualHash := hex.EncodeToString(hasher.Sum(nil))
		if actualHash != info.SHA256 {
			return fmt.Errorf("checksum verification failed: expected %s, got %s", info.SHA256, actualHash)
		}
		fmt.Println("Checksum verified successfully")
	}

	// Move temp file to final location
	if err := os.Rename(tempFile.Name(), binaryPath); err != nil {
		return fmt.Errorf("failed to move binary to final location: %w", err)
	}

	// Make executable on Unix systems
	if runtime.GOOS != "windows" {
		if err := os.Chmod(binaryPath, 0755); err != nil {
			return fmt.Errorf("failed to make binary executable: %w", err)
		}
	}

	fmt.Printf("Successfully downloaded and installed go-polars binary to: %s\n", binaryPath)
	return nil
}

// EnsureBinary ensures the binary exists, downloading it if necessary
func EnsureBinary() error {
	exists, binaryPath, err := CheckBinaryExists()
	if err != nil {
		return fmt.Errorf("failed to check binary existence: %w", err)
	}

	if exists {
		// Binary exists, verify it's not corrupted by trying to get file info
		if stat, err := os.Stat(binaryPath); err == nil && stat.Size() > 0 {
			return nil
		}
	}

	// Binary doesn't exist or is corrupted, try to download it
	if err := DownloadBinary(""); err != nil {
		// If download fails, provide helpful error message
		return fmt.Errorf("binary not found and download failed: %w\n\nPlease either:\n1. Upload the binary to GitHub releases, or\n2. Build from source using: ./build.sh\n3. Or set GO_POLARS_SKIP_DOWNLOAD=true and place the binary manually", err)
	}

	return nil
}

// GetVersionFromEnv gets version from environment variable if set
func GetVersionFromEnv() string {
	if version := os.Getenv("GO_POLARS_VERSION"); version != "" {
		return version
	}
	return getLatestVersion()
}

// CleanOldBinaries removes old binary files to save space
func CleanOldBinaries() error {
	binDir, err := GetBinDirectory()
	if err != nil {
		return err
	}

	// Get current binary name
	info, err := GetBinaryInfo("")
	if err != nil {
		return err
	}

	// Read directory
	entries, err := os.ReadDir(binDir)
	if err != nil {
		return err
	}

	// Remove old binaries (files that look like our binaries but aren't the current one)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Skip current binary
		if name == info.LocalFilename {
			continue
		}

		// Remove old versions (files containing version numbers)
		if strings.Contains(name, "libpolars_go") || strings.Contains(name, "polars_go") {
			oldPath := filepath.Join(binDir, name)
			if err := os.Remove(oldPath); err == nil {
				fmt.Printf("Removed old binary: %s\n", name)
			}
		}
	}

	return nil
}
