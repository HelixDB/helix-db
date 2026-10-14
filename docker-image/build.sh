#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: docker-image/build.sh --platform PLATFORM --image IMAGE (--load | --output PATH) [--async-index-benchmark]' \
    'Optional base pins: --rust-image IMAGE@sha256:DIGEST --runtime-image IMAGE@sha256:DIGEST' \
    '' \
    'Supported platforms:' \
    '  linux/amd64' \
    '  linux/arm64' >&2
}

platform=""
image=""
mode=""
output_path=""
async_index_benchmark=false
rust_image=""
runtime_image=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform)
      platform="${2:-}"
      shift 2
      ;;
    --image)
      image="${2:-}"
      shift 2
      ;;
    --load)
      if [[ -n "$mode" ]]; then
        printf 'choose exactly one output mode\n' >&2
        exit 2
      fi
      mode="load"
      shift
      ;;
    --output)
      if [[ -n "$mode" ]]; then
        printf 'choose exactly one output mode\n' >&2
        exit 2
      fi
      mode="output"
      output_path="${2:-}"
      shift 2
      ;;
    --async-index-benchmark)
      async_index_benchmark=true
      shift
      ;;
    --rust-image|--runtime-image)
      if [[ ! "${2:-}" =~ @sha256:[[:xdigit:]]{64}$ ]]; then
        printf '%s requires a digest-pinned image reference\n' "$1" >&2
        exit 2
      fi
      if [[ "$1" == --rust-image ]]; then
        rust_image="$2"
      else
        runtime_image="$2"
      fi
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$platform" || -z "$image" || -z "$mode" ]]; then
  usage
  exit 2
fi

case "$platform" in
  linux/amd64|linux/arm64) ;;
  *)
    printf 'unsupported platform: %s\n' "$platform" >&2
    exit 2
    ;;
esac

command -v docker >/dev/null 2>&1 || {
  printf 'missing required command: docker\n' >&2
  exit 1
}
docker buildx version >/dev/null

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
build_args=(
  docker buildx build
  --file "$repo_root/Dockerfile"
  --platform "$platform"
  --tag "$image"
)

cargo_features=""
if [[ "$async_index_benchmark" == true ]]; then
  cargo_features="server/async-index-benchmark"
fi
if [[ -n "$cargo_features" ]]; then
  build_args+=(--build-arg "HELIX_CARGO_FEATURES=$cargo_features")
fi
if [[ -n "$rust_image" ]]; then
  build_args+=(--build-arg "RUST_IMAGE=$rust_image")
fi
if [[ -n "$runtime_image" ]]; then
  build_args+=(--build-arg "RUNTIME_IMAGE=$runtime_image")
fi

case "$mode" in
  load)
    build_args+=(--load)
    ;;
  output)
    if [[ -z "$output_path" ]]; then
      printf '%s\n' '--output requires a path' >&2
      exit 2
    fi
    if [[ -e "$output_path" ]]; then
      printf 'refusing to overwrite image archive: %s\n' "$output_path" >&2
      exit 1
    fi
    output_dir=$(dirname -- "$output_path")
    if [[ ! -d "$output_dir" ]]; then
      printf 'image archive directory does not exist: %s\n' "$output_dir" >&2
      exit 1
    fi
    build_args+=(--output "type=docker,dest=$output_path")
    ;;
esac

build_args+=("$repo_root")
exec "${build_args[@]}"
