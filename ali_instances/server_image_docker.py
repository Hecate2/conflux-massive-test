import argparse
import tarfile
import time
from pathlib import Path

import asyncssh
from loguru import logger

from conflux.config import (
    ConfluxNodeConfig,
    DEFAULT_CHAIN_ID,
    DEFAULT_EVM_CHAIN_ID,
    DEFAULT_EVM_RPC_PORT,
    DEFAULT_EVM_WS_PORT,
    DEFAULT_RPC_PORT,
    DEFAULT_WS_PORT,
    build_single_node_conflux_config_text,
)
from .ecs_common import load_ali_credentials, load_endpoint, wait_for_tcp_port_open
from .image_config import ImageBuildConfig, DEFAULT_KEYPAIR_NAME, DEFAULT_REGION_ID, DEFAULT_SSH_PRIVATE_KEY
from .server_image import create_server_image

DEFAULT_DOCKER_TAG = "conflux-single-node:latest"
DEFAULT_DOCKER_REPO_URL = "https://github.com/Conflux-Chain/conflux-rust.git"
DEFAULT_DOCKER_BRANCH = "v3.0.2"
DEFAULT_DOCKER_CONTEXT = Path(__file__).resolve().parents[1] / "node_docker_image"
DEFAULT_SERVICE_NAME = "conflux-docker"


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _pos_config_source() -> Path:
    return _workspace_root() / "ref" / "zero-gravity-swap" / "pos_config"


def _write_temp_file(content: str, suffix: str) -> Path:
    path = Path(f"/tmp/conflux_{int(time.time())}{suffix}")
    path.write_text(content)
    return path


def _build_docker_run_script(
    docker_tag: str,
    service_name: str,
    config_path: str,
    log_dir: str,
    data_dir: str,
    pos_config_dir: str,
) -> str:
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            f"docker rm -f {service_name} >/dev/null 2>&1 || true",
            "exec docker run --rm \\",
            f"  --name {service_name} \\",
            "  --net=host \\",
            "  --privileged \\",
            "  --ulimit nofile=65535:65535 \\",
            "  --ulimit nproc=65535:65535 \\",
            "  --ulimit core=-1 \\",
            f"  -v {config_path}:{config_path} \\",
            f"  -v {data_dir}:{data_dir} \\",
            f"  -v {log_dir}:{log_dir} \\",
            f"  -v {pos_config_dir}:{pos_config_dir} \\",
            f"  -v {pos_config_dir}:/app/pos_config \\",
            f"  -w {log_dir} \\",
            f"  {docker_tag} \\",
            f"  /root/conflux --config {config_path}/conflux_0.toml",
        ]
    )


def _build_systemd_service(service_name: str) -> str:
    return "\n".join(
        [
            "[Unit]",
            f"Description=Conflux single node ({service_name})",
            "After=docker.service",
            "Requires=docker.service",
            "",
            "[Service]",
            "Type=simple",
            f"ExecStart=/usr/local/bin/{service_name}-run.sh",
            f"ExecStop=/usr/bin/docker stop {service_name}",
            "Restart=always",
            "RestartSec=5",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
        ]
    )


def _make_prepare_instance(
    node_config: ConfluxNodeConfig,
    docker_context: Path,
    docker_tag: str,
    docker_repo_url: str,
    docker_branch: str,
    service_name: str,
):
    async def prepare_instance(host: str, config: ImageBuildConfig) -> None:
        key_path = str(Path(config.ssh_private_key_path).expanduser())
        await wait_for_tcp_port_open(host, 22, timeout=config.wait_timeout, interval=3)
        async with asyncssh.connect(
            host,
            username=config.ssh_username,
            client_keys=[key_path],
            known_hosts=None,
        ) as conn:
            async def run_remote(cmd: str, check: bool = True) -> None:
                logger.info(f"remote: {cmd}")
                result = await conn.run(cmd, check=check)
                if result.stdout:
                    logger.info(result.stdout.strip())
                if result.stderr:
                    logger.warning(result.stderr.strip())

            await run_remote("sudo apt-get update -y")
            await run_remote("sudo apt-get install -y docker.io ca-certificates curl tar")
            await run_remote("sudo systemctl enable --now docker")
            await run_remote(f"sudo mkdir -p {node_config.remote_config_dir}")
            await run_remote(f"sudo mkdir -p {node_config.remote_data_dir}")
            await run_remote(f"sudo mkdir -p {node_config.remote_log_dir}")
            await run_remote(f"sudo mkdir -p {node_config.remote_pos_config_dir}")

            config_text = build_single_node_conflux_config_text(node_config)
            local_config = _write_temp_file(config_text, ".toml")
            remote_config = f"{node_config.remote_config_dir}/conflux_0.toml"
            await asyncssh.scp(str(local_config), (conn, remote_config))
            local_config.unlink(missing_ok=True)

            pos_config_local = _pos_config_source()
            if not pos_config_local.exists():
                raise FileNotFoundError(f"pos_config not found: {pos_config_local}")
            pos_archive = Path(f"/tmp/pos_config_{int(time.time())}.tar.gz")
            with tarfile.open(pos_archive, "w:gz") as tar:
                tar.add(pos_config_local, arcname="pos_config")
            remote_archive = f"/tmp/{pos_archive.name}"
            await asyncssh.scp(str(pos_archive), (conn, remote_archive))
            pos_archive.unlink(missing_ok=True)
            await run_remote(
                f"sudo tar -xzf {remote_archive} -C {node_config.remote_pos_config_dir} --strip-components=1"
            )
            await run_remote(f"sudo rm -f {remote_archive}", check=False)
            await run_remote(f"sudo mkdir -p {node_config.remote_pos_config_dir}/log")

            if not docker_context.exists():
                raise FileNotFoundError(f"docker context not found: {docker_context}")
            context_archive = Path(f"/tmp/conflux_docker_ctx_{int(time.time())}.tar.gz")
            with tarfile.open(context_archive, "w:gz") as tar:
                tar.add(docker_context, arcname=".")
            remote_ctx_archive = f"/tmp/{context_archive.name}"
            await asyncssh.scp(str(context_archive), (conn, remote_ctx_archive))
            context_archive.unlink(missing_ok=True)

            await run_remote("sudo rm -rf /opt/conflux/docker")
            await run_remote("sudo mkdir -p /opt/conflux/docker")
            await run_remote(f"sudo tar -xzf {remote_ctx_archive} -C /opt/conflux/docker")
            await run_remote(f"sudo rm -f {remote_ctx_archive}", check=False)

            cachebust = int(time.time())
            build_cmd = (
                "sudo bash -lc 'set -o pipefail; "
                "DOCKER_BUILDKIT=0 docker build "
                f"--build-arg CACHEBUST={cachebust} "
                f"--build-arg BRANCH={docker_branch} "
                f"--build-arg REPO_URL={docker_repo_url} "
                f"-t {docker_tag} /opt/conflux/docker "
                "2>&1 | tee /tmp/conflux_docker_build.log'"
            )
            try:
                await run_remote(build_cmd)
            except Exception:
                await run_remote("sudo tail -n 200 /tmp/conflux_docker_build.log || true", check=False)
                raise

            run_script = _build_docker_run_script(
                docker_tag,
                service_name,
                node_config.remote_config_dir,
                node_config.remote_log_dir,
                node_config.remote_data_dir,
                node_config.remote_pos_config_dir,
            )
            run_script_path = f"/usr/local/bin/{service_name}-run.sh"
            await run_remote(
                "sudo bash -lc 'cat > "
                + run_script_path
                + " << "
                + "\"EOF\"\n"
                + run_script
                + "\nEOF\n" 
                + f"chmod +x {run_script_path}'"
            )

            unit_text = _build_systemd_service(service_name)
            unit_path = f"/etc/systemd/system/{service_name}.service"
            await run_remote(
                "sudo bash -lc 'cat > "
                + unit_path
                + " << "
                + "\"EOF\"\n"
                + unit_text
                + "\nEOF\n" 
                + "systemctl daemon-reload; systemctl enable "
                + service_name
                + ".service'"
            )

    return prepare_instance


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create Alibaba Cloud image with Conflux single-node Docker image")
    parser.add_argument("--conflux-git-ref", default="v3.0.2", help="Label for the image name")
    parser.add_argument("--base-image-id", default=None)
    parser.add_argument("--instance-type", default=None)
    parser.add_argument("--min-cpu-cores", type=int, default=4)
    parser.add_argument("--min-memory-gb", type=float, default=8.0)
    parser.add_argument("--max-memory-gb", type=float, default=8.0)
    parser.add_argument("--v-switch-id", default=None)
    parser.add_argument("--security-group-id", default=None)
    parser.add_argument("--key-pair-name", default=DEFAULT_KEYPAIR_NAME)
    parser.add_argument("--region-id", default=DEFAULT_REGION_ID)
    parser.add_argument("--zone-id", default=None)
    parser.add_argument("--endpoint", default=None, help="Custom ECS endpoint")
    parser.add_argument("--image-prefix", default="conflux-docker")
    parser.add_argument("--ssh-username", default="root")
    parser.add_argument("--ssh-private-key", default=DEFAULT_SSH_PRIVATE_KEY)
    parser.add_argument("--internet-max-bandwidth-out", type=int, default=10)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--wait-timeout", type=int, default=1800)
    parser.add_argument("--search-all-regions", action="store_true")
    parser.add_argument("--spot", action="store_true")
    parser.add_argument("--spot-strategy", default="SpotAsPriceGo")
    parser.add_argument("--vpc-name", default="conflux-image-builder")
    parser.add_argument("--vswitch-name", default="conflux-image-builder")
    parser.add_argument("--security-group-name", default="conflux-image-builder")
    parser.add_argument("--vpc-cidr", default="10.0.0.0/16")
    parser.add_argument("--vswitch-cidr", default="10.0.0.0/24")
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")

    parser.add_argument("--rpc-port", type=int, default=DEFAULT_RPC_PORT)
    parser.add_argument("--ws-port", type=int, default=DEFAULT_WS_PORT)
    parser.add_argument("--evm-rpc-port", type=int, default=DEFAULT_EVM_RPC_PORT)
    parser.add_argument("--evm-ws-port", type=int, default=DEFAULT_EVM_WS_PORT)
    parser.add_argument("--chain-id", type=int, default=DEFAULT_CHAIN_ID)
    parser.add_argument("--evm-chain-id", type=int, default=DEFAULT_EVM_CHAIN_ID)
    parser.add_argument("--mining-author", default=None)

    parser.add_argument("--docker-context", default=str(DEFAULT_DOCKER_CONTEXT))
    parser.add_argument("--docker-tag", default=DEFAULT_DOCKER_TAG)
    parser.add_argument("--docker-repo-url", default=DEFAULT_DOCKER_REPO_URL)
    parser.add_argument("--docker-branch", default=DEFAULT_DOCKER_BRANCH)
    parser.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    credentials = load_ali_credentials()
    endpoint = args.endpoint or load_endpoint()
    config = ImageBuildConfig(
        credentials=credentials,
        base_image_id=args.base_image_id,
        instance_type=args.instance_type,
        min_cpu_cores=args.min_cpu_cores,
        min_memory_gb=args.min_memory_gb,
        max_memory_gb=args.max_memory_gb,
        v_switch_id=args.v_switch_id,
        security_group_id=args.security_group_id,
        key_pair_name=args.key_pair_name,
        conflux_git_ref=args.conflux_git_ref,
        region_id=args.region_id,
        zone_id=args.zone_id,
        endpoint=endpoint,
        image_prefix=args.image_prefix,
        ssh_username=args.ssh_username,
        ssh_private_key_path=args.ssh_private_key,
        internet_max_bandwidth_out=args.internet_max_bandwidth_out,
        poll_interval=args.poll_interval,
        wait_timeout=args.wait_timeout,
        cleanup_builder_instance=not args.no_cleanup,
        search_all_regions=args.search_all_regions,
        use_spot=args.spot,
        spot_strategy=args.spot_strategy,
        vpc_name=args.vpc_name,
        vswitch_name=args.vswitch_name,
        security_group_name=args.security_group_name,
        vpc_cidr=args.vpc_cidr,
        vswitch_cidr=args.vswitch_cidr,
    )

    node_config = ConfluxNodeConfig(
        rpc_port=args.rpc_port,
        ws_port=args.ws_port,
        evm_rpc_port=args.evm_rpc_port,
        evm_ws_port=args.evm_ws_port,
        chain_id=args.chain_id,
        evm_chain_id=args.evm_chain_id,
        mining_author=args.mining_author,
    )

    prepare_fn = _make_prepare_instance(
        node_config=node_config,
        docker_context=Path(args.docker_context).expanduser(),
        docker_tag=args.docker_tag,
        docker_repo_url=args.docker_repo_url,
        docker_branch=args.docker_branch,
        service_name=args.service_name,
    )

    image_id = create_server_image(config, dry_run=args.dry_run, prepare_instance_fn=prepare_fn)
    logger.info(f"image id: {image_id}")


if __name__ == "__main__":
    main()
