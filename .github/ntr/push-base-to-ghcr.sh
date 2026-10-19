#!/usr/bin/env bash
# Retag a local base JDK image and push it to GitHub Container Registry (GHCR).
#
# Usage:
#   export GITHUB_TOKEN=ghp_...   # PAT with write:packages (and read:packages)
#   export GHCR_USER=devbykidle   # GitHub username / org that owns the package
#   ./.github/ntr/push-base-to-ghcr.sh
#
# Optional overrides:
#   SRC_IMAGE=registry.ntrlab.ru/cbr/axiom-jdk-alpine-musl:17
#   DST_IMAGE=ghcr.io/devbykidle/axiom-jdk-alpine-musl:17
set -euo pipefail

SRC_IMAGE="${SRC_IMAGE:-registry.ntrlab.ru/cbr/axiom-jdk-alpine-musl:17}"
GHCR_OWNER="${GHCR_OWNER:-${GHCR_USER:-devbykidle}}"
DST_IMAGE="${DST_IMAGE:-ghcr.io/${GHCR_OWNER}/axiom-jdk-alpine-musl:17}"
GHCR_USER="${GHCR_USER:-${GHCR_OWNER}}"

if ! docker image inspect "$SRC_IMAGE" >/dev/null 2>&1; then
  echo "error: local image not found: $SRC_IMAGE" >&2
  echo "pull or load it first, then re-run." >&2
  exit 1
fi

echo "Tagging ${SRC_IMAGE} -> ${DST_IMAGE}"
docker tag "$SRC_IMAGE" "$DST_IMAGE"

if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
else
  echo "GITHUB_TOKEN not set; using interactive docker login for ghcr.io"
  docker login ghcr.io
fi

echo "Pushing ${DST_IMAGE}"
docker push "$DST_IMAGE"
echo "OK: ${DST_IMAGE}"
echo
echo "Make the package public (or grant Actions read) under:"
echo "  https://github.com/users/${GHCR_OWNER}/packages/container/axiom-jdk-alpine-musl/settings"
