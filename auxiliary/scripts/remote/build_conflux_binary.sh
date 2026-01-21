#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${1:-/opt/conflux/src/conflux-rust}"
GIT_REF="${2:?git ref required}"

cd "$REPO_DIR"

git fetch --depth 1 origin "$GIT_REF" || true
git checkout "$GIT_REF" || git checkout FETCH_HEAD
git submodule update --init --recursive

curl https://sh.rustup.rs -sSf | sh -s -- -y
if [[ -f "$HOME/.cargo/env" ]]; then
  . "$HOME/.cargo/env"
fi

cargo build --release --bin conflux
install -m 0755 target/release/conflux /usr/local/bin/conflux
