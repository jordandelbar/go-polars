#!/bin/sh

set -e

# go-polars Complete Setup Script
# This script sets up go-polars in your Go project with the precompiled binary
# Usage: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh

REPO="jordandelbar/go-polars"
DEFAULT_VERSION="v0.0.25"
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

while [ $# -gt 0 ]; do
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
            echo "go-polars Complete Setup Script"
            echo ""
            echo "This script sets up go-polars in your Go project with the precompiled binary."
            echo ""
            echo "Usage: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh"
            echo "   Or: curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh -s -- [options]"
            echo ""
            echo "Options:"
            echo "  --version VERSION    Install specific version (default: $DEFAULT_VERSION)"
            echo "  --force             Force reinstall even if already set up"
            echo "  --skip-verify       Skip checksum verification"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  # Set up latest version"
            echo "  curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh"
            echo ""
            echo "  # Set up specific version"
            echo "  curl -sSL https://raw.githubusercontent.com/jordandelbar/go-polars/main/scripts/setup.sh | sh -s -- --version v0.0.22"
            echo ""
            echo "Requirements:"
            echo "  - Run from your Go project root directory"
            echo "  - Linux x86_64 (other platforms need to build from source)"
            echo "  - curl, sha256sum (for verification)"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set default version if not specified
if [ -z "$VERSION" ]; then
    VERSION="$DEFAULT_VERSION"
fi

# Check if we're in a directory suitable for a Go project
check_environment() {
    local current_dir=$(basename "$PWD")

    if [ ! -f "go.mod" ] && [ "$current_dir" = "/" ]; then
        log_error "Please run this script from your Go project directory"
        log_info "If this is a new project, create it first:"
        log_info "  mkdir my-project && cd my-project"
        log_info "  go mod init my-project"
        exit 1
    fi
}

# Detect OS and architecture
detect_platform() {
    local os=""
    local arch=""

    case "$(uname -s)" in
        Linux*)
            os="linux"
            ;;
        Darwin*)
            log_error "macOS precompiled binaries are not yet available"
            log_info "Please build from source:"
            log_info "  git clone https://github.com/$REPO.git"
            log_info "  cd go-polars && ./build.sh"
            exit 1
            ;;
        *)
            log_error "Unsupported operating system: $(uname -s)"
            log_info "Please build from source:"
            log_info "  git clone https://github.com/$REPO.git"
            log_info "  cd go-polars && ./build.sh"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)
            arch="amd64"
            ;;
        *)
            log_error "Unsupported architecture: $(uname -m)"
            log_info "Please build from source for your architecture"
            exit 1
            ;;
    esac

    echo "${os}-${arch}"
}

# Check dependencies
check_dependencies() {
    local missing_deps=""

    if ! command -v curl >/dev/null 2>&1; then
        missing_deps="$missing_deps curl"
    fi

    if ! command -v go >/dev/null 2>&1; then
        missing_deps="$missing_deps go"
    fi

    if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
        missing_deps="$missing_deps sha256sum_or_shasum"
    fi

    if [ -n "$missing_deps" ]; then
        log_error "Missing required dependencies:$missing_deps"
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

    if [ "$SKIP_VERIFY" = "true" ]; then
        log_warning "Skipping checksum verification"
        return 0
    fi

    log_info "Verifying checksum..."

    local expected_checksum
    expected_checksum=$(cut -d' ' -f1 "$checksum_file")

    local actual_checksum
    if command -v sha256sum >/dev/null 2>&1; then
        actual_checksum=$(sha256sum "$file" | cut -d' ' -f1)
    elif command -v shasum >/dev/null 2>&1; then
        actual_checksum=$(shasum -a 256 "$file" | cut -d' ' -f1)
    else
        log_error "No SHA256 utility available"
        return 1
    fi

    if [ "$expected_checksum" = "$actual_checksum" ]; then
        log_success "Checksum verification passed"
        return 0
    else
        log_error "Checksum verification failed"
        log_info "Expected: $expected_checksum"
        log_info "Actual:   $actual_checksum"
        return 1
    fi
}

# Download and extract polars package
setup_polars_package() {
    log_info "Setting up polars package..."

    # Download polars package files if they don't exist
    if [ ! -d "polars" ] || [ "$FORCE" = "true" ]; then
        log_info "Downloading polars package files..."

        local temp_archive="/tmp/go-polars-${VERSION}.tar.gz"
        local extract_dir="/tmp/go-polars-extract-$$"

        # Download the source archive
        if ! download_file "https://github.com/$REPO/archive/main.tar.gz" "$temp_archive"; then
            log_error "Failed to download polars package"
            return 1
        fi

        # Extract polars directory
        mkdir -p "$extract_dir"
        tar -xzf "$temp_archive" -C "$extract_dir" --strip-components=1

        # Copy polars directory to current location
        cp -r "$extract_dir/polars" .

        # Clean up
        rm -rf "$temp_archive" "$extract_dir"

        log_success "Polars package files installed"
    else
        log_info "Polars package already exists"
    fi
}

# Download precompiled binary
setup_binary() {
    local platform="$1"

    # Create bin directory
    mkdir -p "polars/bin"

    local binary_file="polars/bin/$BINARY_NAME"

    # Check if binary already exists
    if [ -f "$binary_file" ] && [ "$FORCE" != "true" ]; then
        log_success "Binary already exists at $binary_file"
        return 0
    fi

    log_info "Downloading precompiled binary for $platform..."

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

    # Download binary
    if ! download_file "$library_url" "$temp_library"; then
        log_error "Failed to download binary"
        log_info "Available releases: https://github.com/$REPO/releases"
        return 1
    fi

    # Download checksum
    if ! download_file "$checksum_url" "$temp_checksum"; then
        log_warning "Could not download checksum file, skipping verification"
    else
        # Verify checksum
        if ! verify_checksum "$temp_library" "$temp_checksum"; then
            log_error "Checksum verification failed"
            return 1
        fi
    fi

    # Install binary
    log_info "Installing binary to $binary_file..."
    cp "$temp_library" "$binary_file"
    chmod 644 "$binary_file"

    log_success "Binary installed successfully"
}

# Setup Go module
setup_go_module() {
    # Initialize go.mod if it doesn't exist
    if [ ! -f "go.mod" ]; then
        local project_name=$(basename "$PWD")
        log_info "Initializing Go module: $project_name"
        go mod init "$project_name"
    fi

    # Add replace directive for local polars package
    log_info "Setting up Go module replace directive..."
    go mod edit -replace="github.com/$REPO=."

    log_success "Go module configured"
}

# Create example file
create_example() {
    if [ ! -f "example.go" ] || [ "$FORCE" = "true" ]; then
        log_info "Creating example file..."
        cat > example.go << 'EOF'
package main

import (
	"fmt"
	"log"

	"github.com/jordandelbar/go-polars/polars"
)

func main() {
	fmt.Println("🚀 go-polars Example")

	// Create a DataFrame
	df, err := polars.NewDataFrame().
		AddStringColumn("name", []string{"Alice", "Bob", "Charlie"}).
		AddIntColumn("age", []int64{25, 30, 35}).
		AddFloatColumn("score", []float64{85.5, 92.0, 78.5}).
		Build()
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Printf("Created DataFrame with %d rows and %d columns\n", df.Height(), df.Width())
	fmt.Println(df.String())

	// Filter and transform
	filtered := df.Filter(polars.Col("age").Gt(28))
	fmt.Printf("\nFiltered (age > 28): %d rows\n", filtered.Height())
	fmt.Println(filtered.String())

	// Add calculated column
	withBonus := df.WithColumns(
		polars.Col("score").MulValue(1.1).Alias("score_with_bonus"),
	)
	fmt.Printf("\nWith bonus column: %d columns\n", withBonus.Width())
	fmt.Println(withBonus.String())

	fmt.Println("\n✅ Example completed successfully!")
}
EOF
        log_success "Example file created: example.go"
    fi
}

# Main setup function
main() {
    log_info "🚀 Starting go-polars complete setup..."
    log_info "Version: $VERSION"

    # Check environment
    check_environment

    # Detect platform
    local platform
    platform=$(detect_platform)
    log_info "Detected platform: $platform"

    # Check dependencies
    check_dependencies

    # Setup polars package
    if ! setup_polars_package; then
        log_error "Failed to setup polars package"
        exit 1
    fi

    # Setup binary
    if ! setup_binary "$platform"; then
        log_error "Failed to setup binary"
        exit 1
    fi

    # Setup Go module
    if ! setup_go_module; then
        log_error "Failed to setup Go module"
        exit 1
    fi

    # Create example
    create_example

    # Add dependency after example is created so it's needed
    log_info "Adding go-polars dependency..."
    go mod edit -require="github.com/$REPO@$VERSION"
    go mod tidy

    # Create build timestamp
    echo "$(date +%s)" > "polars/bin/.build_timestamp"

    log_success "🎉 Setup completed successfully!"
    echo ""
    log_info "📋 What was installed:"
    log_info "  • polars/ - Go package files"
    log_info "  • polars/bin/libpolars_go.a - Precompiled binary ($(du -h polars/bin/libpolars_go.a | cut -f1))"
    log_info "  • go.mod - Updated with replace directive"
    log_info "  • example.go - Sample usage"
    echo ""
    log_info "🚀 Try it out:"
    log_info "  go run example.go"
    echo ""
    log_info "📚 Documentation:"
    log_info "  https://github.com/$REPO"
    echo ""
    log_success "Happy coding with go-polars! 🐻‍❄️"
}

# Run setup
main "$@"
