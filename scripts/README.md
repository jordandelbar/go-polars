# Installation Scripts

This directory contains scripts to help users easily install go-polars with precompiled binaries.

## Scripts Overview

### `setup.sh` - Complete Project Setup (Recommended)

The complete setup script that handles everything needed to get go-polars working in your project.

**Usage:**
```bash
curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh
```

**What it does:**
- Downloads the polars Go package files to your project
- Downloads the precompiled binary for your platform
- Sets up Go module with proper replace directives
- Creates an example file to test the installation
- Handles all dependencies and configuration

**Options:**
- `--version VERSION` - Install specific version (default: v0.0.22)
- `--force` - Force reinstall even if already set up
- `--skip-verify` - Skip checksum verification
- `--help, -h` - Show help message

**Requirements:**
- Linux x86_64 (other platforms need to build from source)
- Run from your Go project root directory
- curl, sha256sum (for verification)

### `install.sh` - Binary-Only Installation

Downloads only the precompiled binary to your current project.

**Usage:**
```bash
curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh
```

**What it does:**
- Downloads the precompiled static library
- Verifies checksums
- Installs to `polars/bin/libpolars_go.a`
- Provides setup instructions

**Options:**
- Same as `setup.sh`

**Use this when:**
- You already have the polars package files
- You only need to update/install the binary
- You want more control over the setup process

## Platform Support

Currently, precompiled binaries are available for:
- ✅ Linux x86_64

For other platforms, you'll need to build from source:
```bash
git clone https://github.com/jordandelbar/go-polars.git
cd go-polars
./build.sh
```

## Security & Verification

All precompiled binaries include SHA256 checksums for verification:

```bash
# Manual verification
sha256sum -c libpolars_go-linux-amd64-v0.0.22.a.sha256
```

The scripts automatically verify checksums unless `--skip-verify` is used.

## Examples

### Quick Start for New Project
```bash
mkdir my-polars-project
cd my-polars-project
curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh
go run example.go
```

### Install Specific Version
```bash
curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh -s -- --version v0.0.21
```

### Force Reinstall
```bash
curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh -s -- --force
```

### Binary Only (for existing projects)
```bash
cd my-existing-project
curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh
```

## Troubleshooting

### "Checksum verification failed"
- Try with `--skip-verify` flag (not recommended for production)
- Check your internet connection
- Verify the release exists on GitHub

### "Unsupported platform"
- Build from source using `./build.sh`
- Check the [releases page](https://github.com/jordandelbar/go-polars/releases) for available platforms

### "Module not found" errors
- Ensure you're in your Go project directory
- Check that `go.mod` has the correct replace directive
- Run `go mod tidy`

### Permission errors
- Ensure you have write permissions in the current directory
- Don't run with sudo (not needed and not recommended)

## Contributing

To add support for new platforms:
1. Update the platform detection in both scripts
2. Ensure releases include binaries for the new platform
3. Test the installation process
4. Update documentation

For issues or improvements, please open an issue or PR in the main repository.
