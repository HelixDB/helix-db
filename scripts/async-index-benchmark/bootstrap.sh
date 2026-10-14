#!/usr/bin/env bash
# Run through SSM on the dedicated Ubuntu 24.04 benchmark instances.
set -euo pipefail
[[ $(id -u) == 0 ]] || { printf 'run as root through SSM\n' >&2; exit 2; }
[[ $(uname -m) == x86_64 ]] || { printf 'native AMD64 required\n' >&2; exit 2; }
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 ]] || exit 2
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates curl docker.io docker-buildx docker-compose-v2 \
  git jq python3 python3-venv sysstat tcpdump zstd
systemctl enable --now docker
install -d /opt/helix-async/evidence /opt/helix-async/fixtures
python3 -m venv /opt/helix-async/venv
/opt/helix-async/venv/bin/pip install \
  numpy==2.2.5 pyarrow==20.0.0 certifi==2025.4.26 awscli==1.46.1
dpkg-query -W > /opt/helix-async/evidence/packages.tsv
/opt/helix-async/venv/bin/pip freeze > /opt/helix-async/evidence/python-packages.txt
uname -a > /opt/helix-async/evidence/kernel.txt
lscpu --json > /opt/helix-async/evidence/cpu.json
lsblk --json --bytes > /opt/helix-async/evidence/storage.json
docker version --format '{{json .}}' > /opt/helix-async/evidence/docker.json
docker buildx version > /opt/helix-async/evidence/buildx.txt
docker compose version > /opt/helix-async/evidence/compose.txt
date --utc --iso-8601=seconds > /opt/helix-async/evidence/bootstrap-complete.txt
