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
    --title "Release $VERSION - Linux Static Library" \
    --notes-file "RELEASE_NOTES.md" \
    libpolars_go-linux-amd64-*.a \
    libpolars_go-linux-amd64-*.a.sha256 \
    libpolars_go-linux-amd64-*.a.md5

echo "✅ Release created successfully!"
echo "🌐 View at: https://github.com/$(gh repo view --json owner,name -q '.owner.login + "/" + .name')/releases/tag/$VERSION"
