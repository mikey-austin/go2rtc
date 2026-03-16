#!/bin/sh

set -eu

if [ $# -lt 1 ] || [ -z "$1" ]; then
    echo "Usage: $0 <image-tag> [platform]" >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker could not be found. Please install it." >&2
    exit 1
fi

tag="$1"
platform="${2:-linux/amd64}"

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

exec docker buildx build \
    --platform "$platform" \
    --load \
    -t "$tag" \
    -f docker/Dockerfile \
    .
