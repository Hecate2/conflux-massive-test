#!/usr/bin/env bash
set -euo pipefail

apt-get update -y
apt-get install -y docker.io ca-certificates curl
systemctl enable --now docker

echo "LABEL=cloudimg-rootfs / ext4 defaults,noatime,nodiratime,barrier=0 0 0" > /tmp/fstab
cp /tmp/fstab /etc/fstab
