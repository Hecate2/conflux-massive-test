#!/bin/bash

# 遇到错误立即退出 (确保第一步失败时脚本停止)
set -e

# 定义 Python 解释器路径
PYTHON="/home/ubuntu/miniconda/bin/python"

# 定义日志分割函数
print_separator() {
    echo ""
    echo "============================================================"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "============================================================"
    echo ""
}

# 定义清理函数 (确保无论第二步成功还是失败，都会执行销毁)
cleanup() {
    # 临时关闭 'set -e'，防止销毁过程中的小错误阻断日志打印
    set +e
    
    # print_separator "正在执行清理操作: 销毁 EC2 实例..."
    # $PYTHON -m ali_instances.cleanup_resources --instances-json ali_servers.json
    
    print_separator "跳过销毁实例"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 脚本执行结束。"
}

# 注册 trap：脚本退出时（无论是正常结束还是出错中断）都会执行 cleanup 函数
trap cleanup EXIT

# --- 步骤 1: 创建实例 ---
print_separator "步骤 1/2: 创建 Aliyun 实例..."
$PYTHON -m ali_instances.create_servers

# --- 步骤 2: 远程模拟 ---
print_separator "步骤 2/2: 开始远程模拟..."
$PYTHON -m remote_simulate

# --- 如果远程模拟成功退出，手动触发清理，并且关闭 trap ---
cleanup
trap - EXIT

# --- 其他操作