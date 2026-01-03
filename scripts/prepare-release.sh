#!/bin/bash

set -euo pipefail

VERSION=""
PLATFORMS=()
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RELEASE_DIR="$PROJECT_ROOT/release"
ARTIFACT_ROWS=()

usage() {
    cat <<'EOF'
Usage: ./scripts/prepare-release.sh [options] [version]

Options:
  --platform <name>    Build for specific platform (repeatable)
                       Supported: linux-amd64, darwin-arm64, darwin-amd64, darwin-universal
  --all                Build all supported platforms available on this host
  -h, --help           Show this help message

Arguments:
  version              Release tag/version (default: current timestamp)

Examples:
  ./scripts/prepare-release.sh v0.0.27
  ./scripts/prepare-release.sh --platform darwin-arm64 v0.0.27
  ./scripts/prepare-release.sh --all
EOF
}

require_tool() {
    local tool="$1"
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "❌ Required tool '$tool' not found" >&2
        exit 1
    fi
}

ensure_prereqs() {
    require_tool cargo
    require_tool rustc
}

compute_sha256() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | cut -d' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file" | cut -d' ' -f1
    else
        echo "❌ No SHA256 utility available" >&2
        exit 1
    fi
}

compute_md5() {
    local file="$1"
    if command -v md5sum >/dev/null 2>&1; then
        md5sum "$file" | cut -d' ' -f1
    elif command -v md5 >/dev/null 2>&1; then
        md5 -q "$file"
    else
        echo "❌ No MD5 utility available" >&2
        exit 1
    fi
}

add_platform_default() {
    if [[ ${#PLATFORMS[@]} -gt 0 ]]; then
        return
    fi

    case "$(uname -s)" in
        Linux*)
            PLATFORMS=("linux-amd64")
            ;;
        Darwin*)
            PLATFORMS=("darwin-arm64" "darwin-amd64")
            ;;
        *)
            echo "❌ Unsupported host OS for automatic platform detection" >&2
            exit 1
            ;;
    esac
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --platform)
                if [[ $# -lt 2 ]]; then
                    echo "❌ Missing value for --platform" >&2
                    exit 1
                fi
                PLATFORMS+=("$2")
                shift 2
                ;;
            --all)
                PLATFORMS=("linux-amd64" "darwin-arm64" "darwin-amd64")
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            --*)
                echo "❌ Unknown option: $1" >&2
                usage
                exit 1
                ;;
            *)
                if [[ -z "$VERSION" ]]; then
                    VERSION="$1"
                    shift
                else
                    echo "❌ Multiple version arguments provided" >&2
                    exit 1
                fi
                ;;
        esac
    done

    if [[ -z "$VERSION" ]]; then
        VERSION="$(date +"%Y%m%d-%H%M%S")"
    fi

    add_platform_default
}

ensure_host_support() {
    local required_os="$1"
    local current_os="$(uname -s)"
    if [[ "$required_os" != "$current_os" ]]; then
        cat <<EOF >&2
❌ Platform requires host OS '$required_os'
   Current host: '$current_os'
   Tip: Run this script on the matching host or build that artifact separately.
EOF
        exit 1
    fi
}

ensure_target_installed() {
    local target_triple="$1"
    if command -v rustup >/dev/null 2>&1; then
        rustup target add "$target_triple" >/dev/null 2>&1 || true
    fi
}

artifact_row() {
    local filename="$1"
    local platform_label="$2"
    local size="$3"
    local sha256="$4"
    local md5="$5"
    ARTIFACT_ROWS+=("$filename|$platform_label|$size|$sha256|$md5")
}

build_platform() {
    local platform="$1"
    local target_triple=""
    local required_os=""
    local platform_label=""
    local extra_step=""

    case "$platform" in
        linux-amd64)
            target_triple="x86_64-unknown-linux-gnu"
            required_os="Linux"
            platform_label="Linux x86_64"
            ;;
        darwin-arm64)
            target_triple="aarch64-apple-darwin"
            required_os="Darwin"
            platform_label="macOS arm64"
            ;;
        darwin-amd64)
            target_triple="x86_64-apple-darwin"
            required_os="Darwin"
            platform_label="macOS x86_64"
            ;;
        darwin-universal)
            required_os="Darwin"
            platform_label="macOS universal"
            extra_step="universal"
            ;;
        *)
            echo "❌ Unsupported platform: $platform" >&2
            exit 1
            ;;
    esac

    ensure_host_support "$required_os"

    if [[ "$extra_step" == "universal" ]]; then
        build_universal "$platform" "$platform_label"
        return
    fi

    ensure_target_installed "$target_triple"

    echo "🔨 Building $platform_label ($target_triple)..."
    cd "$PROJECT_ROOT/polars/bindings"
    cargo build --release --target "$target_triple"

    local artifact_src="$PROJECT_ROOT/polars/bindings/target/$target_triple/release/libpolars_go.a"
    if [[ ! -f "$artifact_src" ]]; then
        echo "❌ Expected artifact not found: $artifact_src" >&2
        exit 1
    fi

    local artifact_name="libpolars_go-${platform}-${VERSION}.a"
    local artifact_dest="$RELEASE_DIR/$artifact_name"

    cp "$artifact_src" "$artifact_dest"

    local size sha md5
    size="$(du -h "$artifact_dest" | cut -f1)"
    sha="$(compute_sha256 "$artifact_dest")"
    md5="$(compute_md5 "$artifact_dest")"

    echo "$sha  $artifact_name" >"$artifact_dest.sha256"
    echo "$md5  $artifact_name" >"$artifact_dest.md5"

    artifact_row "$artifact_name" "$platform_label" "$size" "$sha" "$md5"

    echo "✅ Artifact ready: $artifact_name ($size)"
}

build_universal() {
    local platform="$1"
    local platform_label="$2"
    local arm_artifact="$RELEASE_DIR/libpolars_go-darwin-arm64-${VERSION}.a"
    local x86_artifact="$RELEASE_DIR/libpolars_go-darwin-amd64-${VERSION}.a"

    if [[ ! -f "$arm_artifact" || ! -f "$x86_artifact" ]]; then
        cat <<EOF >&2
❌ Universal build requires both arm64 and x86_64 macOS artifacts.
   Missing files:
     - $arm_artifact
     - $x86_artifact
   Build them first (order matters).
EOF
        exit 1
    fi

    require_tool lipo

    local artifact_name="libpolars_go-${platform}-${VERSION}.a"
    local artifact_dest="$RELEASE_DIR/$artifact_name"

    echo "🔗 Creating universal macOS archive..."
    lipo -create -output "$artifact_dest" "$arm_artifact" "$x86_artifact"

    local size sha md5
    size="$(du -h "$artifact_dest" | cut -f1)"
    sha="$(compute_sha256 "$artifact_dest")"
    md5="$(compute_md5 "$artifact_dest")"

    echo "$sha  $artifact_name" >"$artifact_dest.sha256"
    echo "$md5  $artifact_name" >"$artifact_dest.md5"

    artifact_row "$artifact_name" "$platform_label" "$size" "$sha" "$md5"

    echo "✅ Artifact ready: $artifact_name ($size)"
}

write_release_notes() {
    local release_notes="$RELEASE_DIR/RELEASE_NOTES.md"

    {
        echo "# go-polars Release $VERSION"
        echo
        echo "## Artifacts"
        echo
        echo "| File | Platform | Size | SHA256 | MD5 |"
        echo "| ---- | -------- | ---- | ------ | --- |"
        for row in "${ARTIFACT_ROWS[@]}"; do
            IFS='|' read -r file platform_label size sha md5 <<<"$row"
            echo "| \`$file\` | $platform_label | $size | \`$sha\` | \`$md5\` |"
        done
        echo
        echo "## Verification"
        echo
        echo "For each downloaded artifact, run:"
        echo '```bash'
        echo 'sha256sum -c <artifact>.sha256    # or: shasum -a 256 -c'
        echo '```'
        echo
        echo "## Build Information"
        echo
        echo "- Built on: $(date -u)"
        echo "- Host: $(uname -a)"
        echo "- Rust: $(rustc --version)"
        echo "- Polars crate: $(cd "$PROJECT_ROOT/polars/bindings" && cargo tree | grep -m1 '\bpolars v' | awk '{print $2}')"
    } >"$release_notes"
}

main() {
    parse_args "$@"
    ensure_prereqs

    echo "🚀 Preparing release $VERSION"
    echo "📁 Output directory: $RELEASE_DIR"
    rm -rf "$RELEASE_DIR"
    mkdir -p "$RELEASE_DIR"

    echo "📦 Building platforms: ${PLATFORMS[*]}"
    for platform in "${PLATFORMS[@]}"; do
        build_platform "$platform"
    done

    write_release_notes

    echo
    echo "🎉 Release artifacts ready in $RELEASE_DIR"
    ls -1 "$RELEASE_DIR"
    echo
    echo "🚀 Upload with:"
    echo "  cd $RELEASE_DIR && ../scripts/upload-github-release.sh $VERSION"
}

main "$@"
