#!/bin/bash

set -e

echo "🔧 Building go-polars library..."

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p polars/bin

echo "🦀 Compiling Rust bindings..."
cd polars/bindings

cargo clean
cargo build --release

echo "📦 Copying binary to bin directory..."
cd "$SCRIPT_DIR"

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    cp polars/bindings/target/release/libpolars_go.so polars/bin/libpolars_go.so
    echo "✅ Linux library built successfully!"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    cp polars/bindings/target/release/libpolars_go.dylib polars/bin/libpolars_go.dylib
    echo "✅ macOS library built successfully!"
elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    cp polars/bindings/target/release/polars_go.dll polars/bin/polars_go.dll
    echo "✅ Windows library built successfully!"
else
    echo "❌ Unsupported operating system: $OSTYPE"
    exit 1
fi

echo "🎉 Build completed successfully!"
echo ""
echo "📋 You can now run the examples:"
echo "   make run-basic-example"
echo "   make run-expressions-example"
