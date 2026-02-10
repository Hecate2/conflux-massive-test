from remote_simulation.port_allocation import p2p_port, rpc_port, pubsub_port, remote_rpc_port, evm_rpc_port, evm_rpc_ws_port

# REMOTE_IMAGE_TAG = "public.ecr.aws/s9d3x9f5/conflux-massive-test/conflux-node:latest"
REMOTE_IMAGE_TAG = "lylcx2007/conflux-node:latest"
IMAGE_TAG = "conflux-node:latest"
REGISTRY_IMAGE = "conflux-node:base"
REGISTRY_PORT = 5000

CONTAINER_PREFIX = "conflux_node_"

def container_name(index: int) -> str:
    return f"{CONTAINER_PREFIX}{index}"

def collect_log_container_name(index: int) -> str:
    return f"{CONTAINER_PREFIX}{index}"

def launch_node(index: int) -> str:
    port_cmd = (
        f"-p {p2p_port(index)}:{p2p_port(0)}",
        f"-p {rpc_port(index)}:{rpc_port(0)}",
        f"-p {pubsub_port(index)}:{pubsub_port(0)}",
        f"-p {remote_rpc_port(index)}:{remote_rpc_port(0)}",
        f"-p {evm_rpc_port(index)}:{evm_rpc_port(0)}",
        f"-p {evm_rpc_ws_port(index)}:{evm_rpc_ws_port(0)}"
    )

    cmd_startup = (
        f"sudo rm -rf ~/log{index} &&",
        f"mkdir ~/log{index} &&",
        "sudo docker run -d",
        f"--name {container_name(index)}",          
        "-v ~/config.toml:/root/config.toml:ro",
        f"-v ~/log{index}:/root/log",
        "--privileged",                          # 如果需要调整内核参数或高权限
        *port_cmd,
        "-w /root",                              # 设置工作目录
        IMAGE_TAG, 
        "./conflux --config /root/config.toml"
    )

    return " ".join(cmd_startup)

def stop_node_and_collect_log(index: int, *, user = "ubuntu") -> str:
    stop_node = (
        f"sudo docker stop {container_name(index)}",
    )
    collect_logs = (
        f"sudo rm -rf ~/output{index} &&",
        f"mkdir ~/output{index} &&",
        "sudo docker run --rm",
        f"--name {container_name(index)}_collect",          
        f"-v ~/log{index}:/root/log:ro",
        f"-v ~/output{index}:/root/output",
        "-w /root",                       
        IMAGE_TAG, 
        "/bin/bash -c ./collect_logs.sh &&",
        f"sudo chown {user}:{user} ~/output{index}/*"
    )

    return " ".join(stop_node) + " && " + " ".join(collect_logs)

def stop_all_nodes() -> str:
    return f"sudo docker ps -aq --filter name={CONTAINER_PREFIX} | xargs -r sudo docker stop"

def destory_all_nodes() -> str:
    return f"sudo docker ps -aq --filter name={CONTAINER_PREFIX} | xargs -r sudo docker rm -f && sudo rm -rf ~/log* && sudo rm -rf ~/output*"

def _ensure_registry_running() -> str:
    return " && ".join(
        (
            "sudo systemctl start docker",
            "sudo systemctl is-active --quiet docker || sudo systemctl restart docker",
            (
                "if ! sudo docker ps -a --format '{{.Names}}' | grep -qx conflux-registry; "
                "then sudo docker run -d --restart=always --name conflux-registry "
                "-p 5000:5000 -v /opt/registry/data:/var/lib/registry registry:2; fi"
            ),
            "sudo docker start conflux-registry >/dev/null 2>&1 || true",
        )
    )


def _configure_insecure_registry(registry_host: str) -> str:
    registry_entry = f"{registry_host}:{REGISTRY_PORT}"
    local_entry = f"localhost:{REGISTRY_PORT}"
    daemon_json = (
        f"{{\\\"insecure-registries\\\": [\\\"{registry_entry}\\\", \\\"{local_entry}\\\"]}}"
    )
    return " && ".join(
        (
            "sudo mkdir -p /etc/docker",
            f"sudo bash -c \"printf '%s\\n' '{daemon_json}' > /etc/docker/daemon.json\"",
            "sudo systemctl restart docker",
        )
    )


def _wait_registry_ready(registry_host: str) -> str:
    return (
        f"for i in $(seq 1 40); do "
        f"curl -fsS http://{registry_host}:{REGISTRY_PORT}/v2/ >/dev/null && break; "
        f"sleep 3; "
        f"done"
    )


def pull_image_from_dockerhub_and_push_local() -> str:
    registry_image = f"localhost:{REGISTRY_PORT}/{REGISTRY_IMAGE}"
    return " && ".join(
        (
            _ensure_registry_running(),
            _wait_registry_ready("localhost"),
            f"sudo docker pull {REMOTE_IMAGE_TAG}",
            f"sudo docker tag {REMOTE_IMAGE_TAG} {IMAGE_TAG}",
            f"sudo docker tag {REMOTE_IMAGE_TAG} {registry_image}",
            f"sudo docker push {registry_image}",
        )
    )


def pull_image_from_registry_and_push_local(registry_host: str) -> str:
    remote_registry_image = f"{registry_host}:{REGISTRY_PORT}/{REGISTRY_IMAGE}"
    local_registry_image = f"localhost:{REGISTRY_PORT}/{REGISTRY_IMAGE}"
    return " && ".join(
        (
            _configure_insecure_registry(registry_host),
            _ensure_registry_running(),
            _wait_registry_ready(registry_host),
            f"sudo docker pull {remote_registry_image}",
            f"sudo docker tag {remote_registry_image} {IMAGE_TAG}",
            f"sudo docker tag {remote_registry_image} {local_registry_image}",
            _wait_registry_ready("localhost"),
            f"sudo docker push {local_registry_image}",
        )
    )


def pull_image():
    return f"sudo docker pull {REMOTE_IMAGE_TAG} && sudo docker tag {REMOTE_IMAGE_TAG} {IMAGE_TAG}"