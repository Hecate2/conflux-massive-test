#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${1:?service name required}"
IMAGE_TAG="${2:-conflux-single-node:latest}"
CONFLUX_BIN="${3:-/root/conflux}"
CONFIG_PATH="${4:-/opt/conflux/config/conflux_0.toml}"

RUN_SCRIPT="/usr/local/bin/${SERVICE_NAME}-run.sh"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [[ ! -f "$SERVICE_FILE" ]]; then
  cat > "$RUN_SCRIPT" << EOF
#!/usr/bin/env bash
set -euo pipefail

docker rm -f ${SERVICE_NAME} >/dev/null 2>&1 || true
exec docker run --rm \
  --name ${SERVICE_NAME} \
  --net=host \
  --privileged \
  --ulimit nofile=65535:65535 \
  --ulimit nproc=65535:65535 \
  --ulimit core=-1 \
  -v /opt/conflux/config:/opt/conflux/config \
  -v /opt/conflux/data:/opt/conflux/data \
  -v /opt/conflux/logs:/opt/conflux/logs \
  -v /opt/conflux/pos_config:/opt/conflux/pos_config \
  -v /opt/conflux/pos_config:/app/pos_config \
  -w /opt/conflux/logs \
  ${IMAGE_TAG} \
  ${CONFLUX_BIN} --config ${CONFIG_PATH}
EOF

  chmod +x "$RUN_SCRIPT"

  cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Conflux single node (${SERVICE_NAME})
After=docker.service
Requires=docker.service

[Service]
Type=simple
ExecStart=${RUN_SCRIPT}
ExecStop=/usr/bin/docker stop ${SERVICE_NAME}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable ${SERVICE_NAME}.service
fi
