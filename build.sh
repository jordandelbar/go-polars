#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BUILD_TIMESTAMP_FILE="polars/bin/.build_timestamp"

# Function to check if we need to rebuild
needs_rebuild() {
    local binary_path=""
    local source_dirs=("polars/bindings/src" "polars")
    local config_files=("polars/bindings/Cargo.toml" "polars/bindings/Cargo.lock")

    # Determine binary path based on OS
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        binary_path="polars/bin/libpolars_go.a"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        binary_path="polars/bin/libpolars_go.a"
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
        binary_path="polars/bin/polars_go.lib"
    fi

    # Check if binary exists
    if [[ ! -f "$binary_path" ]]; then
        echo "🔍 Binary not found: $binary_path"
        return 0  # needs rebuild
    fi

    # Check if any source files are newer than binary
    for dir in "${source_dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            while IFS= read -r -d '' file; do
                if [[ "$file" -nt "$binary_path" ]]; then
                    echo "🔍 Source file newer than binary: $file"
                    return 0  # needs rebuild
                fi
            done < <(find "$dir" -type f \( -name "*.rs" -o -name "*.go" -o -name "*.h" \) -print0)
        fi
    done

    # Check if config files are newer than binary
    for file in "${config_files[@]}"; do
        if [[ -f "$file" && "$file" -nt "$binary_path" ]]; then
            echo "🔍 Config file newer than binary: $file"
            return 0  # needs rebuild
        fi
    done

    # Check build timestamp file
    if [[ -f "$BUILD_TIMESTAMP_FILE" && -f "$binary_path" ]]; then
        local last_build=$(cat "$BUILD_TIMESTAMP_FILE" 2>/dev/null || echo "0")
        local current_time=$(date +%s)
        local time_diff=$((current_time - last_build))

        # If build is older than 1 hour and there are changes, suggest rebuild
        if [[ $time_diff -gt 3600 ]]; then
            echo "🕐 Build is over 1 hour old ($(($time_diff / 60)) minutes)"
        fi
    fi

    return 1  # no rebuild needed
}

# Check if check mode is requested
if [[ "$1" == "--check" ]]; then
    if needs_rebuild; then
        echo "🔄 Rebuild would be required"
        exit 1
    else
        echo "✅ Binary is up to date"
        exit 0
    fi
fi

# Check if force rebuild is requested
if [[ "$1" == "--force" ]] || [[ "$1" == "-f" ]]; then
    echo "🔧 Force rebuild requested..."
    FORCE_REBUILD=true
elif needs_rebuild; then
    echo "🔧 Changes detected, rebuilding go-polars library..."
    FORCE_REBUILD=false
else
    echo "✅ Binary is up to date, skipping rebuild"
    echo "💡 Use --force to rebuild anyway"
    exit 0
fi

if ! command -v cargo &> /dev/null; then
    echo "❌ Cargo is not installed. Please install Rust first:"
    echo "   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

if ! command -v make &> /dev/null; then
    echo "❌ Make is not installed. Please install build-essential:"
    echo "   sudo apt-get install build-essential"
    exit 1
fi

mkdir -p polars/bin

echo "🦀 Compiling Rust bindings..."
cd polars/bindings

# Only clean if force rebuild or if we detect significant changes
if [[ "$FORCE_REBUILD" == "true" ]]; then
    echo "🧹 Cleaning previous build..."
    cargo clean
fi

cargo build --release

echo "📦 Copying binary to bin directory..."
cd "$SCRIPT_DIR"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    cp polars/bindings/target/release/libpolars_go.a polars/bin/libpolars_go.a
    echo "✅ Linux static library built successfully!"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    cp polars/bindings/target/release/libpolars_go.a polars/bin/libpolars_go.a
    echo "✅ macOS static library built successfully!"
elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    cp polars/bindings/target/release/polars_go.lib polars/bin/polars_go.lib
    echo "✅ Windows static library built successfully!"
else
    echo "❌ Unsupported operating system: $OSTYPE"
    exit 1
fi

# Save build timestamp
echo "$(date +%s)" > "$BUILD_TIMESTAMP_FILE"

echo "🎉 Build completed successfully!"
echo ""
echo "📋 You can now run the examples:"
echo "   make run-basic-example"
echo "   make run-expressions-example"
echo "   make run-groupby-example"
