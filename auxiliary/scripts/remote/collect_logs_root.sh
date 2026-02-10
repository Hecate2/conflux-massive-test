#!/usr/bin/env bash
set -euo pipefail

INDEX="${1:?node index required}"
IMAGE_TAG="${2:-conflux-node:latest}"
CONTAINER="conflux_node_${INDEX}"

if docker ps -aq --filter "name=^${CONTAINER}$" | grep -q .; then
  docker stop "$CONTAINER" || true
fi

rm -rf "/root/output${INDEX}"
mkdir -p "/root/output${INDEX}"

docker run --rm --name "${CONTAINER}_collect" \
  -v "/root/log${INDEX}:/root/log:ro" \
  -v "/root/output${INDEX}:/root/output" \
  -w /root \
  "$IMAGE_TAG" \
  /bin/bash -c "./collect_logs.sh"

# Compress output directory into a 7z archive (max compression). Prefer '7zz' if available.
ARCHIVE="/root/output${INDEX}.7z"
if command -v 7zz >/dev/null 2>&1; then
  7zz a -t7z -mx=9 "$ARCHIVE" "/root/output${INDEX}"
elif command -v 7z >/dev/null 2>&1; then
  7z a -t7z -mx=9 "$ARCHIVE" "/root/output${INDEX}"
else
  echo "Warning: 7z not available, skipping compression" >&2
fi
# Remove uncompressed output to save space only if the archive was successfully created
# if [ -s "${ARCHIVE}" ]; then
#   rm -rf "/root/output${INDEX}"
# else
#   echo "Warning: Archive ${ARCHIVE} missing or empty; keeping /root/output${INDEX}" >&2
# fi

