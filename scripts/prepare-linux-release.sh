#!/bin/bash

set -e

VERSION=${1:-$(date +"%Y%m%d-%H%M%S")}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RELEASE_DIR="$PROJECT_ROOT/release"

echo "🚀 Preparing Linux release for go-polars version: $VERSION"

# Check if binary exists
BINARY_PATH="$PROJECT_ROOT/polars/bin/libpolars_go.so"
if [[ ! -f "$BINARY_PATH" ]]; then
    echo "❌ Binary not found at: $BINARY_PATH"
    echo "💡 Run './build.sh --force' first to build the binary"
    exit 1
fi

# Create release directory
echo "📁 Creating release directory..."
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

# Copy and rename the binary
RELEASE_BINARY="$RELEASE_DIR/libpolars_go-linux-amd64-${VERSION}.so"
cp "$BINARY_PATH" "$RELEASE_BINARY"

# Get binary info
BINARY_SIZE=$(du -h "$RELEASE_BINARY" | cut -f1)
BINARY_MD5=$(md5sum "$RELEASE_BINARY" | cut -d' ' -f1)
BINARY_SHA256=$(sha256sum "$RELEASE_BINARY" | cut -d' ' -f1)

echo "✅ Binary copied: $(basename "$RELEASE_BINARY") (${BINARY_SIZE})"

# Create checksums file
echo "🔐 Generating checksums..."
cd "$RELEASE_DIR"
echo "$BINARY_SHA256  $(basename "$RELEASE_BINARY")" > "$(basename "$RELEASE_BINARY").sha256"
echo "$BINARY_MD5  $(basename "$RELEASE_BINARY")" > "$(basename "$RELEASE_BINARY").md5"

# Create release notes
RELEASE_NOTES="$RELEASE_DIR/RELEASE_NOTES.md"
cat > "$RELEASE_NOTES" << EOF
# go-polars Linux Release $VERSION

## Binary Information

- **File**: \`$(basename "$RELEASE_BINARY")\`
- **Platform**: Linux x86_64
- **Size**: $BINARY_SIZE
- **SHA256**: \`$BINARY_SHA256\`
- **MD5**: \`$BINARY_MD5\`

## Installation

1. Download the binary file
2. Verify the checksum:
   \`\`\`bash
   sha256sum -c $(basename "$RELEASE_BINARY").sha256
   \`\`\`
3. Copy to your project:
   \`\`\`bash
   mkdir -p polars/bin
   cp $(basename "$RELEASE_BINARY") polars/bin/libpolars_go.so
   \`\`\`

## Build Information

- **Built on**: $(date -u)
- **Polars version**: $(cd "$PROJECT_ROOT/polars/bindings" && cargo tree | grep "polars v" | head -n1 | awk '{print $2}')
- **Rust version**: $(rustc --version)
- **Build machine**: $(uname -a)

## Usage

After installation, you can use go-polars in your Go projects. Make sure the library is in your library path or use the provided Go bindings that handle loading automatically.

EOF

# Create upload script
UPLOAD_SCRIPT="$RELEASE_DIR/upload-to-github.sh"
cat > "$UPLOAD_SCRIPT" << 'EOF'
#!/bin/bash

# This script helps upload the release to GitHub
# Make sure you have 'gh' CLI installed and authenticated

set -e

VERSION="$1"
if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 v0.1.0"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Creating GitHub release $VERSION..."

# Check if gh CLI is available
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) is not installed"
    echo "💡 Install it from: https://cli.github.com/"
    exit 1
fi

# Create the release
gh release create "$VERSION" \
    --title "Release $VERSION - Linux Binary" \
    --notes-file "RELEASE_NOTES.md" \
    libpolars_go-linux-amd64-*.so \
    libpolars_go-linux-amd64-*.so.sha256 \
    libpolars_go-linux-amd64-*.so.md5

echo "✅ Release created successfully!"
echo "🌐 View at: https://github.com/$(gh repo view --json owner,name -q '.owner.login + "/" + .name')/releases/tag/$VERSION"
EOF

chmod +x "$UPLOAD_SCRIPT"

echo ""
echo "🎉 Release preparation complete!"
echo ""
echo "📂 Release directory: $RELEASE_DIR"
echo "📋 Files created:"
ls -la "$RELEASE_DIR"
echo ""
echo "🚀 To upload to GitHub:"
echo "   1. Make sure you have GitHub CLI installed: https://cli.github.com/"
echo "   2. Authenticate: gh auth login"
echo "   3. Run: cd $RELEASE_DIR && ./upload-to-github.sh $VERSION"
echo ""
echo "📝 Or manually create a release at:"
echo "   https://github.com/jordandelbar/go-polars/releases/new"
echo "   And upload the files from: $RELEASE_DIR"
