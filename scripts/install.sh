#!/bin/bash

set -e

# go-polars Installation Script
# Usage: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh
# Or: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh -s -- --version v0.0.22

REPO="jordandelbar/go-polars"
DEFAULT_VERSION="v0.0.22"
INSTALL_DIR="polars/bin"
BINARY_NAME="libpolars_go.a"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Parse command line arguments
VERSION=""
FORCE=false
SKIP_VERIFY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --skip-verify)
            SKIP_VERIFY=true
            shift
            ;;
        --help|-h)
            echo "go-polars Installation Script"
            echo ""
            echo "Usage: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh"
            echo "   Or: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh -s -- [options]"
            echo ""
            echo "Options:"
            echo "  --version VERSION    Install specific version (default: $DEFAULT_VERSION)"
            echo "  --force             Force reinstall even if library exists"
            echo "  --skip-verify       Skip checksum verification"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  # Install latest version in current project"
            echo "  curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh"
            echo ""
            echo "  # Install specific version"
            echo "  curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh -s -- --version v0.0.22"
            echo ""
            echo "  # Force reinstall"
            echo "  curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/install.sh | sh -s -- --force"
            echo ""
            echo "Note: This script installs the library in your current project directory."
            echo "      Run this from the root of your Go project where you want to use go-polars."
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set default version if not specified
if [[ -z "$VERSION" ]]; then
    VERSION="$DEFAULT_VERSION"
fi

# Detect OS and architecture
detect_platform() {
    local os=""
    local arch=""

    case "$(uname -s)" in
        Linux*)
            os="linux"
            ;;
        Darwin*)
            os="darwin"
            ;;
        CYGWIN*|MINGW*|MSYS*)
            os="windows"
            ;;
        *)
            log_error "Unsupported operating system: $(uname -s)"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)
            arch="amd64"
            ;;
        aarch64|arm64)
            arch="arm64"
            ;;
        *)
            log_error "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac

    echo "${os}-${arch}"
}

# Check if required tools are available
check_dependencies() {
    local missing_deps=()

    if ! command -v curl >/dev/null 2>&1; then
        missing_deps+=("curl")
    fi

    if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
        missing_deps+=("sha256sum or shasum")
    fi

    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing required dependencies: ${missing_deps[*]}"
        log_info "Please install the missing dependencies and try again"
        exit 1
    fi
}

# Download file with progress
download_file() {
    local url="$1"
    local output="$2"

    log_info "Downloading $(basename "$output")..."
    if ! curl -L --progress-bar --fail "$url" -o "$output"; then
        log_error "Failed to download $url"
        return 1
    fi
}

# Verify checksum
verify_checksum() {
    local file="$1"
    local checksum_file="$2"

    if [[ "$SKIP_VERIFY" == "true" ]]; then
        log_warning "Skipping checksum verification"
        return 0
    fi

    log_info "Verifying checksum..."

    # Read expected checksum from file
    local expected_checksum
    expected_checksum=$(cut -d' ' -f1 "$checksum_file")

    # Calculate actual checksum
    local actual_checksum
    if command -v sha256sum >/dev/null 2>&1; then
        actual_checksum=$(sha256sum "$file" | cut -d' ' -f1)
    elif command -v shasum >/dev/null 2>&1; then
        actual_checksum=$(shasum -a 256 "$file" | cut -d' ' -f1)
    else
        log_error "No SHA256 utility available"
        return 1
    fi

    if [[ "$expected_checksum" == "$actual_checksum" ]]; then
        log_success "Checksum verification passed"
        return 0
    else
        log_error "Checksum verification failed"
        log_info "Expected: $expected_checksum"
        log_info "Actual:   $actual_checksum"
        log_warning "You can skip verification with --skip-verify flag, but this is not recommended"
        return 1
    fi
}

# Main installation function
install_library() {
    log_info "Starting go-polars installation..."
    log_info "Version: $VERSION"

    # Detect platform
    local platform
    platform=$(detect_platform)
    log_info "Detected platform: $platform"

    # Check dependencies
    check_dependencies

    # Currently only Linux AMD64 is supported in releases
    if [[ "$platform" != "linux-amd64" ]]; then
        log_error "Precompiled binaries are currently only available for Linux AMD64"
        log_info "For other platforms, please build from source:"
        log_info "  git clone https://github.com/$REPO.git"
        log_info "  cd go-polars"
        log_info "  ./build.sh"
        log_info "  cp polars/bin/libpolars_go.a /path/to/your/project/polars/bin/"
        exit 1
    fi

    # Create install directory
    if [[ ! -d "$INSTALL_DIR" ]]; then
        log_info "Creating directory: $INSTALL_DIR"
        mkdir -p "$INSTALL_DIR"
    fi

    # Check if library already exists
    local target_file="$INSTALL_DIR/$BINARY_NAME"
    if [[ -f "$target_file" && "$FORCE" != "true" ]]; then
        log_success "Library already exists at $target_file"
        log_info "Use --force to reinstall"
        exit 0
    fi

    # Construct download URLs
    local base_url="https://github.com/$REPO/releases/download/$VERSION"
    local library_filename="libpolars_go-${platform}-${VERSION}.a"
    local library_url="$base_url/$library_filename"
    local checksum_url="$base_url/$library_filename.sha256"

    # Create temporary directory
    local temp_dir
    temp_dir=$(mktemp -d)
    trap "rm -rf $temp_dir" EXIT

    local temp_library="$temp_dir/$library_filename"
    local temp_checksum="$temp_dir/$library_filename.sha256"

    # Download library
    if ! download_file "$library_url" "$temp_library"; then
        log_error "Failed to download library from $library_url"
        log_info "Available releases: https://github.com/$REPO/releases"
        exit 1
    fi

    # Download checksum
    if ! download_file "$checksum_url" "$temp_checksum"; then
        log_warning "Could not download checksum file, skipping verification"
        SKIP_VERIFY=true
    fi

    # Verify checksum
    if [[ "$SKIP_VERIFY" != "true" ]]; then
        if ! verify_checksum "$temp_library" "$temp_checksum"; then
            exit 1
        fi
    fi

    # Install library
    log_info "Installing library to $target_file..."
    cp "$temp_library" "$target_file"
    chmod 644 "$target_file"

    log_success "Installation completed successfully!"
    log_info "Library installed at: $target_file"

    # Provide setup instructions
    if [[ -f "go.mod" ]]; then
        log_success "Detected Go module!"
        log_info "To use go-polars in your project:"
        log_info "  1. Add a replace directive to your go.mod:"
        log_info "     go mod edit -replace=github.com/$REPO=."
        log_info "  2. Add the polars package files to your project:"
        log_info "     curl -sSL https://github.com/$REPO/archive/main.tar.gz | tar -xz --strip=2 'go-polars-main/polars'"
        log_info "  3. Get the dependency:"
        log_info "     go get github.com/$REPO@$VERSION"
        log_info "  4. You can now import: github.com/$REPO/polars"
    else
        log_info "To use go-polars in your project:"
        log_info "  1. Initialize a Go module: go mod init your-project-name"
        log_info "  2. Follow the steps above"
    fi

    # Create build timestamp
    echo "$(date +%s)" > "$INSTALL_DIR/.build_timestamp"

    log_success "Installation complete!"
    log_info "💡 Example usage: https://github.com/$REPO/blob/main/examples/test_installation.go"
}

# Run installation
install_library
