#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: docker-image/tests/compose-smoke.sh --platform PLATFORM --image IMAGE\n' >&2
}

platform=""
image=""
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

case "$platform" in
  linux/amd64|linux/arm64) ;;
  *)
    usage
    exit 2
    ;;
esac
if [[ -z "$image" ]]; then
  usage
  exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
fixtures_dir="$script_dir/fixtures"
compose_file="$fixtures_dir/docker-compose.yml"
port=${HELIX_IMAGE_COMPOSE_PORT:-18120}
project="helixdb-image-compose-${RANDOM}-$$"
mc_image="minio/mc:RELEASE.2025-08-13T08-35-41Z@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727"

log() {
  printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

compose() {
  HELIX_IMAGE_REF="$image" \
  HELIX_IMAGE_PLATFORM="$platform" \
  HELIX_IMAGE_TEST_PORT="$port" \
    docker compose -p "$project" -f "$compose_file" "$@"
}

cleanup() {
  set +e
  compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

for command in docker curl python3; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'missing required command: %s\n' "$command" >&2
    exit 1
  }
done
docker version >/dev/null
docker compose version >/dev/null
docker image inspect "$image" >/dev/null

wait_for_http() {
  local url=$1
  local deadline=$((SECONDS + 120))
  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      compose logs --no-color >&2 || true
      printf 'timed out waiting for %s\n' "$url" >&2
      return 1
    fi
    sleep 1
  done
}

post_json() {
  local fixture=$1
  local await_durable=${2:-false}
  local headers=(-H 'content-type: application/json')
  if [[ "$await_durable" == "true" ]]; then
    headers+=(-H 'x-helix-await-durable: true')
  fi
  curl -fsS -X POST "http://127.0.0.1:${port}/v2/query" \
    "${headers[@]}" \
    --data @"$fixtures_dir/$fixture"
}

assert_users_nonempty() {
  local payload=$1
  JSON_PAYLOAD="$payload" python3 - <<'PY'
import json
import os

value = json.loads(os.environ["JSON_PAYLOAD"])["users"]
if isinstance(value, dict) and "properties" in value:
    value = value["properties"]
if not isinstance(value, (list, dict)) or len(value) == 0:
    raise SystemExit(f"expected non-empty users collection, got {value!r}")
PY
}

list_minio_bucket() {
  local bucket=$1
  docker run --rm \
    --platform "$platform" \
    --network "${project}_default" \
    --entrypoint /bin/sh \
    "$mc_image" \
    -c 'until mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; do sleep 1; done; mc ls --recursive "local/$1"' \
    _ "$bucket"
}

assert_minio_object_placement() {
  local main_objects
  local wal_objects
  main_objects=$(list_minio_bucket helix-db)
  wal_objects=$(list_minio_bucket helix-wal)
  if [[ "$main_objects" != *"db/manifest/"* ]]; then
    printf '%s\n' "$main_objects" >&2
    printf 'expected the main bucket to contain db/manifest objects\n' >&2
    exit 1
  fi
  if [[ "$main_objects" == *"db/wal/"* ]]; then
    printf '%s\n' "$main_objects" >&2
    printf 'expected the main bucket not to contain db/wal objects\n' >&2
    exit 1
  fi
  if [[ "$wal_objects" != *"db/wal/"* ]]; then
    printf '%s\n' "$wal_objects" >&2
    printf 'expected the WAL bucket to contain db/wal objects\n' >&2
    exit 1
  fi
  if [[ "$wal_objects" == *"db/manifest/"* || "$wal_objects" == *"db/compacted/"* || "$wal_objects" == *"db/compactions/"* ]]; then
    printf '%s\n' "$wal_objects" >&2
    printf 'expected the WAL bucket not to contain main-store objects\n' >&2
    exit 1
  fi
}

log "Starting pinned MinIO Compose fixture"
compose up -d >/dev/null
wait_for_http "http://127.0.0.1:${port}/healthz"
wait_for_http "http://127.0.0.1:${port}/readyz"
post_json dynamic-write.json true >/dev/null
assert_users_nonempty "$(post_json dynamic-read.json)"
assert_minio_object_placement

log "Replacing Helix while preserving MinIO"
compose up -d --force-recreate helix >/dev/null
wait_for_http "http://127.0.0.1:${port}/readyz"
assert_users_nonempty "$(post_json dynamic-read.json)"

log "Restarting the complete stack without deleting its volume"
compose down >/dev/null
compose up -d >/dev/null
wait_for_http "http://127.0.0.1:${port}/readyz"
assert_users_nonempty "$(post_json dynamic-read.json)"
assert_minio_object_placement
log "S3-compatible Compose smoke test passed"
