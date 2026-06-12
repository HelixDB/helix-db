#!/usr/bin/env bash
# Publish the HelixDB Python SDK (helix-db) to PyPI.
# Usage:
#   PYPI_TOKEN=pypi-xxxx ./publish.sh            # publish to PyPI
#   PYPI_TOKEN=pypi-xxxx ./publish.sh --test     # publish to TestPyPI
set -euo pipefail

cd "$(dirname "$0")"

REPO_ARG=""
INDEX_NAME="PyPI"
if [[ "${1:-}" == "--test" ]]; then
  REPO_ARG="--repository testpypi"
  INDEX_NAME="TestPyPI"
fi

if [[ -z "${PYPI_TOKEN:-}" ]]; then
  echo "error: set PYPI_TOKEN (a PyPI API token, starts with 'pypi-')" >&2
  exit 1
fi

# Prefer python3, fall back to python.
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"
if [[ -z "${PYTHON}" ]]; then
  echo "error: no python3/python interpreter found on PATH" >&2
  exit 1
fi

echo ">> Ensuring build tooling is installed"
"${PYTHON}" -m pip install --quiet --upgrade build twine

echo ">> Cleaning previous artifacts"
rm -rf dist build ./*.egg-info src/*.egg-info

echo ">> Building sdist + wheel"
"${PYTHON}" -m build

echo ">> Validating artifacts"
twine check dist/*

echo ">> Uploading to ${INDEX_NAME}"
TWINE_USERNAME=__token__ TWINE_PASSWORD="${PYPI_TOKEN}" \
  twine upload ${REPO_ARG} dist/*

echo ">> Done. Verify at https://pypi.org/project/helix-db/"
