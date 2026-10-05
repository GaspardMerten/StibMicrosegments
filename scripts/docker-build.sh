#!/usr/bin/env bash
# Local image build against a working copy of the microsegments package (not yet on GitHub / PyPI).
# The copy is staged without .git / .venv / caches, so the build context stays small, and passed as
# the `microsegments-src` named build context (see Dockerfile).
#   scripts/docker-build.sh [path/to/microsegments] [image tag]
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
src="${1:-$here/../microsegments}"
tag="${2:-stibms:local}"
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
rsync -a --exclude .git --exclude .venv --exclude '__pycache__' --exclude .pytest_cache \
      --exclude .hypothesis --exclude .ruff_cache --exclude tests --exclude examples "$src/" "$stage/"
version="$(cd "$src" && python3 -c 'import re,pathlib; p=pathlib.Path("src/microsegments/_version.py"); m=re.search(r"__version__ = version = .([^\x27]+)", p.read_text()) if p.exists() else None; print(m.group(1) if m else "0.0.0")')"
docker build --build-context "microsegments-src=$stage" --build-arg "MS_VERSION=$version" -t "$tag" "$here"
