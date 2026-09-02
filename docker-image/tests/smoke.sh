#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: docker-image/tests/smoke.sh --platform PLATFORM --image IMAGE\n' >&2
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
base_port=${HELIX_IMAGE_TEST_BASE_PORT:-18080}
resource_suffix="${RANDOM}-$$"
containers=()
volumes=()

log() {
  printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

cleanup() {
  set +e
  for container in "${containers[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  for volume in "${volumes[@]}"; do
    docker volume rm -f "$volume" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

for command in docker curl python3; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'missing required command: %s\n' "$command" >&2
    exit 1
  }
done
docker version >/dev/null
docker image inspect "$image" >/dev/null

start_container() {
  local name=$1
  local port=$2
  shift 2

  docker run -d \
    --platform "$platform" \
    --name "$name" \
    -p "${port}:8080" \
    "$@" \
    "$image" >/dev/null
  containers+=("$name")
}

wait_for_http() {
  local url=$1
  local container=$2
  local deadline=$((SECONDS + 90))

  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$container")" != "true" ]]; then
      docker logs "$container" >&2 || true
      printf 'container exited before %s became ready\n' "$url" >&2
      return 1
    fi
    if (( SECONDS >= deadline )); then
      docker logs "$container" >&2 || true
      printf 'timed out waiting for %s\n' "$url" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_exit() {
  local container=$1
  local deadline=$((SECONDS + 30))

  while [[ "$(docker inspect -f '{{.State.Running}}' "$container")" == "true" ]]; do
    if (( SECONDS >= deadline )); then
      docker logs "$container" >&2 || true
      printf 'timed out waiting for %s to exit\n' "$container" >&2
      return 1
    fi
    sleep 1
  done
}

assert_nonzero_exit() {
  local container=$1
  local exit_code
  exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$container")
  if [[ "$exit_code" == "0" ]]; then
    docker logs "$container" >&2 || true
    printf 'expected non-zero exit code for %s\n' "$container" >&2
    exit 1
  fi
}

post_json() {
  local port=$1
  local fixture=$2
  local await_durable=${3:-false}
  local headers=(-H 'content-type: application/json')
  if [[ "$await_durable" == "true" ]]; then
    headers+=(-H 'x-helix-await-durable: true')
  fi
  curl -fsS -X POST "http://127.0.0.1:${port}/v2/query" \
    "${headers[@]}" \
    --data @"$fixtures_dir/$fixture"
}

assert_users_state() {
  local expected=$1
  local payload=$2
  JSON_PAYLOAD="$payload" python3 - "$expected" <<'PY'
import json
import os
import sys

value = json.loads(os.environ["JSON_PAYLOAD"])["users"]
if isinstance(value, dict) and "properties" in value:
    value = value["properties"]
if not isinstance(value, (list, dict)):
    raise SystemExit("users response is not a collection")
is_empty = len(value) == 0
expected = sys.argv[1]
valid = (is_empty if expected == "empty" else not is_empty) if expected in ("empty", "nonempty") else len(value) == int(expected)
if not valid:
    raise SystemExit(f"expected users to be {sys.argv[1]}, got {value!r}")
PY
}

run_memory_test() {
  local first_port=$base_port
  local second_port=$((base_port + 1))
  local first="helixdb-image-memory-a-$resource_suffix"
  local second="helixdb-image-memory-b-$resource_suffix"

  log "Testing memory-mode query round trip and replacement data loss"
  start_container "$first" "$first_port"
  wait_for_http "http://127.0.0.1:${first_port}/healthz" "$first"
  wait_for_http "http://127.0.0.1:${first_port}/readyz" "$first"
  post_json "$first_port" dynamic-write.json true >/dev/null
  assert_users_state nonempty "$(post_json "$first_port" dynamic-read.json)"
  docker rm -f "$first" >/dev/null

  start_container "$second" "$second_port"
  wait_for_http "http://127.0.0.1:${second_port}/readyz" "$second"
  assert_users_state empty "$(post_json "$second_port" dynamic-read.json)"
}

run_native_disk_test() {
  local first_port=$((base_port + 2))
  local second_port=$((base_port + 3))
  local first="helixdb-image-disk-a-$resource_suffix"
  local second="helixdb-image-disk-b-$resource_suffix"
  local third="helixdb-image-disk-c-$resource_suffix"
  local volume="helixdb-image-data-$resource_suffix"
  local mount="type=volume,source=$volume,target=/var/lib/helix"

  log "Testing native-volume persistence"
  docker volume create "$volume" >/dev/null
  volumes+=("$volume")
  start_container "$first" "$first_port" -e HELIX_DATA_DIR=/var/lib/helix --mount "$mount"
  wait_for_http "http://127.0.0.1:${first_port}/readyz" "$first"
  post_json "$first_port" dynamic-write.json true >/dev/null
  assert_users_state nonempty "$(post_json "$first_port" dynamic-read.json)"
  docker stop "$first" >/dev/null
  docker rm "$first" >/dev/null

  start_container "$second" "$second_port" -e HELIX_DATA_DIR=/var/lib/helix --mount "$mount"
  wait_for_http "http://127.0.0.1:${second_port}/readyz" "$second"
  assert_users_state nonempty "$(post_json "$second_port" dynamic-read.json)"

  log "Testing membership deletion, cold restart, and reinsertion"
  post_json "$second_port" dynamic-write.json true >/dev/null
  assert_users_state 2 "$(post_json "$second_port" dynamic-read.json)"
  post_json "$second_port" dynamic-delete.json true >/dev/null
  assert_users_state empty "$(post_json "$second_port" dynamic-read.json)"
  docker stop "$second" >/dev/null
  docker rm "$second" >/dev/null

  start_container "$third" "$first_port" -e HELIX_DATA_DIR=/var/lib/helix --mount "$mount"
  wait_for_http "http://127.0.0.1:${first_port}/readyz" "$third"
  assert_users_state empty "$(post_json "$first_port" dynamic-read.json)"
  post_json "$first_port" dynamic-write.json true >/dev/null
  assert_users_state 1 "$(post_json "$first_port" dynamic-read.json)"
  docker restart "$third" >/dev/null
  wait_for_http "http://127.0.0.1:${first_port}/readyz" "$third"
  assert_users_state 1 "$(post_json "$first_port" dynamic-read.json)"
}

run_invalid_configuration_tests() {
  local bad_address="helixdb-image-bad-address-$resource_suffix"
  local conflicting_storage="helixdb-image-conflicting-storage-$resource_suffix"
  local partial_wal="helixdb-image-partial-wal-$resource_suffix"

  log "Testing invalid startup configuration"
  start_container "$bad_address" "$((base_port + 4))" -e HELIX_HTTP_ADDR=not-an-address
  wait_for_exit "$bad_address"
  assert_nonzero_exit "$bad_address"

  start_container "$conflicting_storage" "$((base_port + 5))" \
    -e HELIX_DATA_DIR=/var/lib/helix \
    -e S3_BUCKET=conflict
  wait_for_exit "$conflicting_storage"
  assert_nonzero_exit "$conflicting_storage"

  start_container "$partial_wal" "$((base_port + 6))" -e WAL_S3_REGION=us-east-1
  wait_for_exit "$partial_wal"
  assert_nonzero_exit "$partial_wal"
}

run_signal_test() {
  local port=$((base_port + 7))
  local container="helixdb-image-sigterm-$resource_suffix"
  local exit_code

  log "Testing graceful SIGTERM shutdown"
  start_container "$container" "$port"
  wait_for_http "http://127.0.0.1:${port}/readyz" "$container"
  docker stop "$container" >/dev/null
  exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$container")
  if [[ "$exit_code" != "0" ]]; then
    docker logs "$container" >&2 || true
    printf 'expected zero exit code after docker stop, got %s\n' "$exit_code" >&2
    exit 1
  fi
}

run_concurrent_membership_test() {
  local port=$((base_port + 7))
  local container="helixdb-image-concurrent-membership-$resource_suffix"

  log "Testing concurrent shared label, equality, adjacency, and cascade membership"
  start_container "$container" "$port"
  wait_for_http "http://127.0.0.1:${port}/readyz" "$container"
  python3 "$script_dir/concurrent_membership.py" --url "http://127.0.0.1:${port}"
}

run_memory_test
run_native_disk_test
run_concurrent_membership_test
run_invalid_configuration_tests
run_signal_test
log "Docker image runtime smoke tests passed"
