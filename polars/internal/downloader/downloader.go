package downloader

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	defaultVersion = "v0.1.0-rc4"
	defaultRepo    = "isesword/go-polars"
)

// Options configures Ensure.
type Options struct {
	Version    string
	Force      bool
	SkipVerify bool
	BinDir     string
	BaseURL    string
	HTTPClient *http.Client
}

// Ensure downloads the correct static library for the host platform if needed.
func Ensure(opts Options) error {
	if opts.Version == "" {
		opts.Version = versionFromEnv()
	}
	if opts.BaseURL == "" {
		opts.BaseURL = fmt.Sprintf("https://github.com/%s/releases/download", defaultRepo)
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Minute}
	}

	osTag, archTag, err := runnerStylePlatform()
	if err != nil {
		return err
	}

	if opts.BinDir == "" {
		opts.BinDir = defaultBinDir(opts.Version, osTag, archTag)
	}

	if err := os.MkdirAll(opts.BinDir, 0o755); err != nil {
		return fmt.Errorf("create bin dir: %w", err)
	}

	binName := binaryName()
	targetPath := filepath.Join(opts.BinDir, binName)
	statePath := filepath.Join(opts.BinDir, ".lib_version")

	if !opts.Force && fileMatchesVersion(targetPath, statePath, opts.Version) {
		return nil
	}

	assetName := assetFilename(osTag, archTag)
	assetURL := fmt.Sprintf("%s/%s/%s", opts.BaseURL, opts.Version, assetName)
	checksumURL := assetURL + ".sha256"

	fmt.Printf("⬇️  downloading %s\n", assetName)
	tmpFile, err := os.CreateTemp("", "polars-*.a")
	if err != nil {
		return fmt.Errorf("tmp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if err := downloadTo(client, assetURL, tmpFile); err != nil {
		return err
	}

	if !opts.SkipVerify {
		checksum, err := fetchChecksum(client, checksumURL)
		if err != nil {
			if errors.Is(err, errChecksumMissing) {
				fmt.Printf("ℹ️  checksum missing, skipping verify\n")
			} else {
				return err
			}
		} else if err := verifyFile(tmpFile.Name(), checksum); err != nil {
			return err
		}
	}

	if err := copyFile(tmpFile.Name(), targetPath); err != nil {
		return err
	}

	if err := os.WriteFile(statePath, []byte(opts.Version), 0o644); err != nil {
		return fmt.Errorf("write version state: %w", err)
	}

	fmt.Printf("✅ %s ready in %s\n", binName, opts.BinDir)
	return nil
}

func versionFromEnv() string {
	if v := os.Getenv("POLARS_BINARY_VERSION"); v != "" {
		return v
	}
	return defaultVersion
}

// defaultBinDir chooses the target directory for the downloaded library.
// Priority:
// 1) POLARS_BIN_DIR (explicit override)
// 2) User cache: <cache>/go-polars/<version>/<os>-<arch>
// 3) Fallback to repo-local polars/libs when cache path is unavailable.
func defaultBinDir(version, osTag, archTag string) string {
	if dir := os.Getenv("POLARS_BIN_DIR"); dir != "" {
		return dir
	}

	if cacheRoot, err := os.UserCacheDir(); err == nil {
		return filepath.Join(cacheRoot, "go-polars", version, fmt.Sprintf("%s-%s", osTag, archTag))
	}

	return filepath.Join("polars", "libs")
}

// runnerStylePlatform returns OS/arch tags used by CI artifact naming, e.g. Linux-X64 or macOS-ARM64.
func runnerStylePlatform() (string, string, error) {
	var osTag, archTag string
	switch runtime.GOOS {
	case "linux":
		osTag = "Linux"
	case "darwin":
		osTag = "macOS"
	case "windows":
		osTag = "Windows"
	default:
		return "", "", fmt.Errorf("unsupported OS %s", runtime.GOOS)
	}

	switch runtime.GOARCH {
	case "amd64":
		archTag = "X64"
	case "arm64":
		archTag = "ARM64"
	default:
		return "", "", fmt.Errorf("unsupported arch %s", runtime.GOARCH)
	}

	return osTag, archTag, nil
}

// assetFilename builds the release asset name matching CI uploads.
// Always uses base name libpolars_go with OS/arch suffix; extension varies by platform.
func assetFilename(osTag, archTag string) string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("libpolars_go-%s-%s.lib", osTag, archTag)
	}
	return fmt.Sprintf("libpolars_go-%s-%s.a", osTag, archTag)
}

func binaryName() string {
	if runtime.GOOS == "windows" {
		return "libpolars_go.lib"
	}
	return "libpolars_go.a"
}

func fileMatchesVersion(binPath, statePath, version string) bool {
	if _, err := os.Stat(binPath); err != nil {
		return false
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == version
}

func downloadTo(client *http.Client, url string, dst *os.File) error {
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("release asset missing: %s", url)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", url, resp.Status)
	}

	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind temp file: %w", err)
	}

	return nil
}

var errChecksumMissing = errors.New("checksum missing")

func fetchChecksum(client *http.Client, url string) (string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("download checksum: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", errChecksumMissing
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("checksum request failed: %s", resp.Status)
	}

	scanner := bufio.NewScanner(resp.Body)
	if !scanner.Scan() {
		return "", errors.New("empty checksum file")
	}
	line := scanner.Text()
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", errors.New("invalid checksum format")
	}
	return fields[0], nil
}

func verifyFile(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open for verify: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hash file: %w", err)
	}

	sum := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(sum, expected) {
		return fmt.Errorf("checksum mismatch: have %s want %s", sum, expected)
	}
	return nil
}

func copyFile(src, dst string) error {
	input, err := os.Open(src)
	if err != nil {
		return err
	}
	defer input.Close()

	tmp, err := os.CreateTemp(filepath.Dir(dst), "polars-lib-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := io.Copy(tmp, input); err != nil {
		tmp.Close()
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp.Name(), dst); err != nil {
		return err
	}

	return nil
}
