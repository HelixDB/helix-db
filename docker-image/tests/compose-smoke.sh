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
# Static identity from the Compose fixture's SeaweedFS S3 config.
s3_bucket="helix-db"
s3_access_key="helix"
s3_secret_key="helix-local-secret"

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

assert_seaweedfs_objects_present() {
  local container=$1
  local objects
  objects=$(docker exec "$container" curl -fsS --max-time 30 \
    --aws-sigv4 aws:amz:us-east-1:s3 --user "${s3_access_key}:${s3_secret_key}" \
    "http://127.0.0.1:8333/${s3_bucket}?list-type=2&prefix=db/manifest/")
  if [[ "$objects" != *"<Key>db/manifest/"* ]]; then
    printf '%s\n' "$objects" >&2
    printf 'expected SeaweedFS to contain db/manifest objects\n' >&2
    exit 1
  fi
}

log "Pulling pinned SeaweedFS and request-trace dependencies"
compose pull seaweedfs s3-trace

log "Starting pinned SeaweedFS Compose fixture"
compose up -d >/dev/null
seaweedfs_container=$(compose ps -q seaweedfs)
log "Probing S3 conditional writes directly and through the request trace"
python3 "$script_dir/s3_conditional_writes.py" \
  --container "$seaweedfs_container" \
  --endpoint http://127.0.0.1:8333 \
  --endpoint http://s3-trace:8333 \
  --bucket "$s3_bucket" \
  --access-key "$s3_access_key" \
  --secret-key "$s3_secret_key"
wait_for_http "http://127.0.0.1:${port}/healthz"
wait_for_http "http://127.0.0.1:${port}/readyz"
post_json dynamic-write.json true >/dev/null
assert_users_nonempty "$(post_json dynamic-read.json)"
assert_seaweedfs_objects_present "$seaweedfs_container"

log "Replacing Helix while preserving SeaweedFS"
compose up -d --force-recreate helix >/dev/null
wait_for_http "http://127.0.0.1:${port}/readyz"
assert_users_nonempty "$(post_json dynamic-read.json)"

log "Restarting the complete stack without deleting its volume"
compose down >/dev/null
compose up -d >/dev/null
wait_for_http "http://127.0.0.1:${port}/readyz"
assert_users_nonempty "$(post_json dynamic-read.json)"
log "Checking idle vector refresh and post-write search"
python3 "$script_dir/vector_idle_refresh.py" --port "$port" --project "$project"
log "S3-compatible Compose smoke test passed"
