#!/bin/bash

set -e

# Pre-release hook to ensure binaries are built and tested before tagging
# This script should be run before creating a release tag

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "🔍 Pre-release validation starting..."

# Check if we're on a clean git state
if ! git diff --quiet; then
    echo "❌ Working directory is not clean. Please commit or stash changes."
    exit 1
fi

# Check if we're on main/master branch
CURRENT_BRANCH=$(git branch --show-current)
if [[ "$CURRENT_BRANCH" != "main" && "$CURRENT_BRANCH" != "master" ]]; then
    echo "⚠️  Warning: Not on main/master branch (current: $CURRENT_BRANCH)"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Aborted"
        exit 1
    fi
fi

# Ensure we have the latest changes
echo "📥 Fetching latest changes..."
git fetch origin

# Check if local branch is up to date
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u})
if [ $LOCAL != $REMOTE ]; then
    echo "❌ Local branch is not up to date with remote. Please pull latest changes."
    exit 1
fi

# Run tests if they exist
if [ -f "go.mod" ]; then
    echo "🧪 Running Go tests..."
    go test ./... || {
        echo "❌ Tests failed"
        exit 1
    }
fi

# Build the library
echo "🔨 Building library..."
./build.sh --force || {
    echo "❌ Build failed"
    exit 1
}

# Verify the binary was created
BINARY_FOUND=false
if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -f "polars/bin/libpolars_go.so" ]]; then
    BINARY_FOUND=true
    BINARY_PATH="polars/bin/libpolars_go.so"
elif [[ "$OSTYPE" == "darwin"* ]] && [[ -f "polars/bin/libpolars_go.dylib" ]]; then
    BINARY_FOUND=true
    BINARY_PATH="polars/bin/libpolars_go.dylib"
elif [[ -f "polars/bin/polars_go.dll" ]]; then
    BINARY_FOUND=true
    BINARY_PATH="polars/bin/polars_go.dll"
fi

if [[ "$BINARY_FOUND" == "false" ]]; then
    echo "❌ Binary not found after build"
    exit 1
fi

echo "✅ Binary created: $BINARY_PATH"

# Basic smoke test - check if binary is valid
if command -v file &> /dev/null; then
    echo "🔍 Binary info:"
    file "$BINARY_PATH"
fi

# Check binary size (warn if too large)
BINARY_SIZE=$(stat -f%z "$BINARY_PATH" 2>/dev/null || stat -c%s "$BINARY_PATH" 2>/dev/null || echo "0")
BINARY_SIZE_MB=$((BINARY_SIZE / 1024 / 1024))

if [[ $BINARY_SIZE_MB -gt 100 ]]; then
    echo "⚠️  Warning: Binary is quite large (${BINARY_SIZE_MB}MB)"
    echo "   Consider optimizing build settings or enabling strip = true in Cargo.toml"
fi

# Run examples if they exist
if [[ -f "Makefile" ]]; then
    echo "🧪 Testing examples..."
    if make run-basic-example >/dev/null 2>&1; then
        echo "✅ Basic example works"
    else
        echo "⚠️  Basic example failed (this might be expected if data files are missing)"
    fi
fi

# Check for common issues
echo "🔍 Checking for common issues..."

# Check if Cargo.lock is committed
if [[ -f "polars/bindings/Cargo.lock" ]] && ! git ls-files --error-unmatch polars/bindings/Cargo.lock >/dev/null 2>&1; then
    echo "⚠️  Warning: Cargo.lock is not committed. Consider adding it for reproducible builds."
fi

# Check for debug symbols in release build
if [[ "$OSTYPE" == "linux-gnu"* ]] && command -v objdump &> /dev/null; then
    if objdump -h "$BINARY_PATH" | grep -q ".debug"; then
        echo "⚠️  Warning: Debug symbols found in binary. Build might not be properly stripped."
    fi
fi

# Suggest next steps
echo ""
echo "✅ Pre-release validation passed!"
echo ""
echo "📋 Next steps:"
echo "   1. Update version in relevant files if needed"
echo "   2. Create and push a git tag:"
echo "      git tag -a v1.0.0 -m 'Release v1.0.0'"
echo "      git push origin v1.0.0"
echo "   3. Or use the release script:"
echo "      ./scripts/build-release.sh 1.0.0"
echo ""
echo "💡 Tips:"
echo "   - Binary
