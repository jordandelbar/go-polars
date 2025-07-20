#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
VERSION=""
DRAFT="false"
PRERELEASE="false"

# Help function
show_help() {
    cat << EOF
Upload GitHub Release Script

USAGE:
    $0 [OPTIONS] VERSION

ARGUMENTS:
    VERSION         Release version (e.g., v0.0.12)

OPTIONS:
    -d, --draft     Create as draft release
    -p, --prerelease Mark as prerelease
    -h, --help      Show this help message

EXAMPLES:
    $0 v0.0.12                    # Create regular release
    $0 -d v0.0.12                 # Create draft release
    $0 -p v0.1.0-beta1            # Create prerelease

PREREQUISITES:
    - GitHub CLI (gh) must be installed and authenticated
    - Release artifacts must be prepared in release/ directory
    - Run prepare-linux-release.sh first

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--draft)
            DRAFT="true"
            shift
            ;;
        -p|--prerelease)
            PRERELEASE="true"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        -*)
            echo -e "${RED}Error: Unknown option $1${NC}"
            show_help
            exit 1
            ;;
        *)
            if [[ -z "$VERSION" ]]; then
                VERSION="$1"
            else
                echo -e "${RED}Error: Multiple versions specified${NC}"
                show_help
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [[ -z "$VERSION" ]]; then
    echo -e "${RED}Error: Version is required${NC}"
    show_help
    exit 1
fi

# Validate version format
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-.*)?$ ]]; then
    echo -e "${YELLOW}Warning: Version format doesn't match semantic versioning (vX.Y.Z)${NC}"
    echo -e "${YELLOW}Provided: $VERSION${NC}"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RELEASE_DIR="$PROJECT_ROOT/release"

echo -e "${BLUE}🚀 GitHub Release Upload Script${NC}"
echo -e "${BLUE}=================================${NC}"
echo
echo "Version: $VERSION"
echo "Draft: $DRAFT"
echo "Prerelease: $PRERELEASE"
echo "Release directory: $RELEASE_DIR"
echo

# Check if GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}❌ GitHub CLI (gh) is not installed${NC}"
    echo
    echo "Install it from: https://cli.github.com/"
    echo
    echo "On most systems:"
    echo "  # Ubuntu/Debian"
    echo "  sudo apt install gh"
    echo
    echo "  # macOS"
    echo "  brew install gh"
    echo
    echo "  # Fedora/CentOS"
    echo "  sudo dnf install gh"
    exit 1
fi

# Check if authenticated
if ! gh auth status &>/dev/null; then
    echo -e "${RED}❌ Not authenticated with GitHub${NC}"
    echo
    echo "Run: gh auth login"
    exit 1
fi

# Check if release directory exists
if [[ ! -d "$RELEASE_DIR" ]]; then
    echo -e "${RED}❌ Release directory not found: $RELEASE_DIR${NC}"
    echo
    echo "Run the prepare script first:"
    echo "  ./scripts/prepare-linux-release.sh $VERSION"
    exit 1
fi

# Change to release directory
cd "$RELEASE_DIR"

# Check for required files
REQUIRED_FILES=(
    "*.a"
    "*.sha256"
    "*.md5"
    "RELEASE_NOTES.md"
)

echo -e "${BLUE}📋 Checking required files...${NC}"
MISSING_FILES=()

for pattern in "${REQUIRED_FILES[@]}"; do
    if ! ls $pattern 1> /dev/null 2>&1; then
        MISSING_FILES+=("$pattern")
    fi
done

if [[ ${#MISSING_FILES[@]} -gt 0 ]]; then
    echo -e "${RED}❌ Missing required files:${NC}"
    for file in "${MISSING_FILES[@]}"; do
        echo "  - $file"
    done
    echo
    echo "Run the prepare script first:"
    echo "  ./scripts/prepare-linux-release.sh $VERSION"
    exit 1
fi

# List files to be uploaded
echo -e "${GREEN}✅ Files ready for upload:${NC}"
ls -la *.a *.sha256 *.md5 2>/dev/null || true
echo

# Confirm upload
echo -e "${YELLOW}🤔 Ready to create GitHub release?${NC}"
echo "Repository: $(gh repo view --json nameWithOwner -q '.nameWithOwner')"
echo "Tag: $VERSION"
echo "Draft: $DRAFT"
echo "Prerelease: $PRERELEASE"
echo
read -p "Proceed with upload? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# Build gh release create command
GH_CMD="gh release create \"$VERSION\""

# Add flags
if [[ "$DRAFT" == "true" ]]; then
    GH_CMD="$GH_CMD --draft"
fi

if [[ "$PRERELEASE" == "true" ]]; then
    GH_CMD="$GH_CMD --prerelease"
fi

# Add title and notes
TITLE="Release $VERSION"
if [[ "$PRERELEASE" == "true" ]]; then
    TITLE="$TITLE (Pre-release)"
fi
if [[ "$DRAFT" == "true" ]]; then
    TITLE="$TITLE (Draft)"
fi

GH_CMD="$GH_CMD --title \"$TITLE\""
GH_CMD="$GH_CMD --notes-file \"RELEASE_NOTES.md\""

# Add files
for file in *.a *.sha256 *.md5; do
    if [[ -f "$file" ]]; then
        GH_CMD="$GH_CMD \"$file\""
    fi
done

echo -e "${BLUE}📤 Creating release...${NC}"
echo "Command: $GH_CMD"
echo

# Execute the command
eval $GH_CMD

if [[ $? -eq 0 ]]; then
    echo
    echo -e "${GREEN}🎉 Release created successfully!${NC}"
    echo
    echo "🌐 View release at:"
    REPO_URL=$(gh repo view --json url -q '.url')
    echo "   $REPO_URL/releases/tag/$VERSION"
    echo

    if [[ "$DRAFT" == "true" ]]; then
        echo -e "${YELLOW}💡 Note: Release is in draft mode${NC}"
        echo "   Edit and publish when ready"
    fi

    echo -e "${BLUE}📖 Next steps:${NC}"
    echo "1. Update download.go defaultVersion to '$VERSION'"
    echo "2. Test the auto-download functionality"
    echo "3. Update documentation if needed"
    echo "4. Announce the release"

else
    echo
    echo -e "${RED}❌ Failed to create release${NC}"
    echo "Check the error message above and try again"
    exit 1
fi
