#!/usr/bin/env bash
set -euo pipefail

CONFLUX_BIN="${1:-/usr/local/bin/conflux}"
SRC_DIR="${2:-/opt/conflux/src/conflux-rust}"

if [[ ! -x "$CONFLUX_BIN" ]]; then
  if [[ -d "$SRC_DIR" ]]; then
    cd "$SRC_DIR"
    if [[ -f "$HOME/.cargo/env" ]]; then
      . "$HOME/.cargo/env"
    fi
    cargo build --release --bin conflux
    install -m 0755 target/release/conflux /usr/local/bin/conflux
  fi
fi

test -x "$CONFLUX_BIN"
