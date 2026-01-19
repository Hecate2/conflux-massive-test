# Stage 1: Build environment
# 必须使用 bookworm (Debian 12)，因为它使用 OpenSSL 3，与 Ubuntu 24.04 兼容
FROM rust:1.77-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive

# 1. 安装编译依赖
RUN apt-get update && apt-get install -y \
    clang \
    git \
    jq \
    libsqlite3-dev \
    xutils-dev \
    cmake \
    pkg-config \
    libssl-dev \
    autoconf \
    libtool \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN rustup install 1.90-x86_64-unknown-linux-gnu
RUN cargo +1.90 install flamegraph

# 2. 准备构建参数
ARG BRANCH=master
ARG REPO_URL=https://github.com/Conflux-Chain/conflux-rust
ARG CACHEBUST=1

WORKDIR /usr/src

# 3. 直接克隆指定的 URL 和 分支
# --depth 1 用于浅克隆，减少下载体积，加快构建速度
RUN git clone --depth 1 --branch "$BRANCH" "$REPO_URL" conflux-rust \
    && cd conflux-rust \
    && git submodule update --init --recursive

WORKDIR /usr/src/conflux-rust

# 4. 编译 Rust 项目
ENV RUSTFLAGS="-g"
ENV CXXFLAGS="-Wno-error=array-bounds -Wno-error"
ENV CFLAGS="-Wno-error=array-bounds -Wno-error"

RUN cargo build --release && \
    cp -r /usr/src/conflux-rust/target/release /tmp/release

# Stage 2: Runtime environment
# 使用 Ubuntu 24.04
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# 1. 安装运行时依赖
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    libsqlite3-0 \
    libssl3 \
    iotop iftop htop \
    jq pssh wget \
    linux-tools-common \
    linux-tools-generic \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. 安装 Python 依赖
# 修正点：Ubuntu 24.04 强制 PEP 668，必须加 --break-system-packages
RUN pip3 install --no-cache-dir --break-system-packages \
    prettytable \
    jsonrpcclient==3.3.6 \
    python-dateutil

# 3. 拷贝 flamegraph
COPY --from=builder /usr/local/cargo/bin/flamegraph /usr/local/bin/flamegraph

# 4. 准备工作目录
WORKDIR /root

# 5. 从 Builder 阶段拷贝产物
COPY --from=builder /tmp/release/conflux /root/conflux
COPY ./scripts/collect_logs.sh /root/
COPY ./scripts/stat_latency_map_reduce.py /root/

# 6. 下载 genesis secrets
# RUN wget -q https://s3-ap-southeast-1.amazonaws.com/conflux-test/genesis_secrets.txt -O /root/genesis_secrets.txt
RUN wget -q https://s3-ap-southeast-1.amazonaws.com/conflux-test/genesis_secrets.txt -O - | head -n 100000 > /root/genesis_secrets.txt


# 设置入口点
CMD ["/bin/bash"]