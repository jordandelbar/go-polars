# Installation Guide

## Quick Start

The easiest way to use go-polars is to simply import it in your Go project. The library will automatically download the appropriate pre-compiled binary for your platform.

```go
package main

import (
    "fmt"
    "github.com/jordandelbar/go-polars/polars"
)

func main() {
    // The binary is automatically downloaded on first import
    df, err := polars.ReadCSV("data.csv")
    if err != nil {
        panic(err)
    }
    fmt.Println(df.String())
}
```

## Automatic Binary Download

When you first import the `polars` package, it will:

1. **Check** if the required binary exists in `polars/bin/`
2. **Download** the appropriate binary for your platform if it doesn't exist
3. **Verify** the binary integrity (when checksums are available)
4. **Cache** the binary for future use

### Supported Platforms

Currently, pre-compiled binaries are available for:

- ✅ **Linux x86_64** (`libpolars_go.so`)
- 🚧 **macOS x86_64** (coming soon)
- 🚧 **Windows x86_64** (coming soon)

## Installation Methods

### Method 1: Automatic Download (Recommended)

```bash
go mod init your-project
go get github.com/jordandelbar/go-polars
```

The binary will be downloaded automatically when you first import the package.

### Method 2: Manual Binary Download

If you prefer to download the binary manually:

1. Go to the [Releases page](https://github.com/jordandelbar/go-polars/releases)
2. Download the binary for your platform
3. Place it in your project's `polars/bin/` directory
4. Rename it to the expected filename:
   - Linux: `libpolars_go.so`
   - macOS: `libpolars_go.dylib`
   - Windows: `polars_go.dll`

### Method 3: Build from Source

If pre-compiled binaries aren't available for your platform:

```bash
git clone https://github.com/jordandelbar/go-polars.git
cd go-polars
./build.sh
```

**Requirements:**
- Rust 1.70+
- Cargo
- C compiler (gcc/clang)

## Configuration

### Environment Variables

- `GO_POLARS_VERSION`: Override the default binary version (default: latest)
- `GO_POLARS_SKIP_DOWNLOAD`: Set to `"true"` to disable automatic downloads

### Example with Custom Version

```bash
export GO_POLARS_VERSION=v0.0.11
go run main.go
```

### Example with Manual Binary Management

```bash
export GO_POLARS_SKIP_DOWNLOAD=true
go run main.go
```

## Verification

### Check if Binary is Available

```go
package main

import (
    "fmt"
    "github.com/jordandelbar/go-polars/polars"
)

func main() {
    if polars.IsInitialized() {
        fmt.Println("✅ go-polars is ready!")
    } else {
        fmt.Printf("❌ Initialization failed: %v\n", polars.GetInitError())
    }
}
```

### Verify Binary Integrity

Downloaded binaries include SHA256 checksums for verification. The library automatically verifies integrity when checksums are available.

## Troubleshooting

### Binary Download Fails

1. **Check internet connection**: The library needs to download from GitHub releases
2. **Check GitHub access**: Ensure you can access `github.com`
3. **Try manual download**: Download and place the binary manually
4. **Build from source**: Use `./build.sh` as a fallback

### Binary Not Found

```
go-polars initialization failed: binary not found
```

**Solution:**
1. Ensure you have internet access for automatic download
2. Or set `GO_POLARS_SKIP_DOWNLOAD=true` and place the binary manually
3. Or build from source using `./build.sh`

### Platform Not Supported

```
unsupported platform: freebsd/amd64
```

**Solution:**
Build from source:
```bash
git clone https://github.com/jordandelbar/go-polars.git
cd go-polars
./build.sh
```

### CGO Errors

```
cgo: C compiler not found
```

**Solution:**
Install a C compiler:
```bash
# Ubuntu/Debian
sudo apt-get install build-essential

# CentOS/RHEL/Fedora
sudo dnf install gcc

# macOS
xcode-select --install
```

## Binary Information

### Current Version: v0.0.12

| Platform | Binary Size | SHA256 |
|----------|-------------|---------|
| Linux x86_64 | ~46MB | `f34e5d9da8f12e0c2c207d98ba7b3d475a6bae632ebb13cf9271d631a6c764b3` |

### Download URLs

Binaries are hosted on GitHub Releases:
```
https://github.com/jordandelbar/go-polars/releases/download/v0.0.12/libpolars_go-linux-amd64-v0.0.12.so
```

## Advanced Usage

### Custom Binary Location

By default, binaries are downloaded to `polars/bin/` within the package directory. This location is automatically managed and you shouldn't need to change it.

### Cleaning Old Binaries

The library automatically cleans up old binary versions to save disk space. You can also manually clean up:

```go
err := polars.CleanOldBinaries()
if err != nil {
    log.Printf("Failed to clean old binaries: %v", err)
}
```

### Force Re-initialization

If you need to force re-download or re-initialize:

```go
err := polars.ForceReinitialize()
if err != nil {
    log.Printf("Re-initialization failed: %v", err)
}
```

## Contributing

To add support for new platforms:

1. Build the binary for the target platform
2. Upload it to a GitHub release
3. Update the `download.go` file with the new platform information
4. Update this documentation

## Security

- All downloads are over HTTPS
- Binary integrity is verified using SHA256 checksums when available
- Binaries are downloaded from the official GitHub repository only
