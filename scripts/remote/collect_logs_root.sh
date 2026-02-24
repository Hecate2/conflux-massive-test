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
