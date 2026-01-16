import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import asyncssh
from loguru import logger
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from utils.wait_until import wait_until
from .ecs_common import (
	DEFAULT_ENDPOINT,
	AliCredentials,
	create_client,
	delete_instance,
	load_ali_credentials,
	load_endpoint,
	pick_zone_id,
	pick_zone_and_instance_type,
	ensure_vpc,
	ensure_vswitch,
	ensure_security_group,
	start_instance,
	stop_instance,
	allocate_public_ip,
	wait_instance_running,
	wait_instance_status,
	wait_for_tcp_port_open,
	create_builder_instance,
	ensure_key_pair,
)

DEFAULT_SECURITY_GROUP_NAME = "conflux-image-builder"
DEFAULT_VPC_CIDR = "10.0.0.0/16"
DEFAULT_VSWITCH_CIDR = "10.0.0.0/24"
DEFAULT_KEYPAIR_NAME = "chenxinghao-conflux-image-builder"
DEFAULT_SSH_PRIVATE_KEY = "./keys/chenxinghao-conflux-image-builder.pem"


@dataclass(frozen=True)
class ImageSource:
	region_id: str
	image_id: str


from .image_config import ImageBuildConfig


def build_image_name(image_prefix: str, conflux_git_ref: str) -> str:
	safe_ref = conflux_git_ref.replace("/", "-").replace(":", "-")
	return f"{image_prefix}-conflux-{safe_ref}"


def list_regions(client: EcsClient) -> Sequence[str]:
	request = ecs_models.DescribeRegionsRequest()
	response = client.describe_regions(request)
	regions = response.body.regions.region if response.body and response.body.regions else []
	return [region.region_id for region in regions if region.region_id]


def ensure_network_resources(client: EcsClient, config: ImageBuildConfig) -> ImageBuildConfig:
	zone_id = config.zone_id or pick_zone_id(client, config.region_id)
	vpc_id = ensure_vpc(client, config.region_id, config.vpc_name, config.vpc_cidr)
	vswitch_id = config.v_switch_id or ensure_vswitch(
		client,
		config.region_id,
		vpc_id,
		zone_id,
		config.vswitch_name,
		config.vswitch_cidr,
		config.vpc_cidr,
	)
	security_group_id = config.security_group_id or ensure_security_group(
		client,
		config.region_id,
		vpc_id,
		config.security_group_name,
		"conflux image builder",
	)
	return ImageBuildConfig(
		credentials=config.credentials,
		base_image_id=config.base_image_id,
		instance_type=config.instance_type,
		min_cpu_cores=config.min_cpu_cores,
		min_memory_gb=config.min_memory_gb,
		max_memory_gb=config.max_memory_gb,
		cpu_vendor=config.cpu_vendor,
		v_switch_id=vswitch_id,
		security_group_id=security_group_id,
		key_pair_name=config.key_pair_name,
		conflux_git_ref=config.conflux_git_ref,
		region_id=config.region_id,
		zone_id=zone_id,
		endpoint=config.endpoint,
		image_prefix=config.image_prefix,
		instance_name_prefix=config.instance_name_prefix,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		ssh_username=config.ssh_username,
		ssh_private_key_path=config.ssh_private_key_path,
		poll_interval=config.poll_interval,
		wait_timeout=config.wait_timeout,
		cleanup_builder_instance=config.cleanup_builder_instance,
		search_all_regions=config.search_all_regions,
		use_spot=config.use_spot,
		spot_strategy=config.spot_strategy,
		vpc_name=config.vpc_name,
		vswitch_name=config.vswitch_name,
		security_group_name=config.security_group_name,
		vpc_cidr=config.vpc_cidr,
		vswitch_cidr=config.vswitch_cidr,
	)


def find_latest_ubuntu_image(client: EcsClient, region_id: str) -> str:
	request = ecs_models.DescribeImagesRequest(
		region_id=region_id,
		image_owner_alias="system",
		ostype="linux",
		architecture="x86_64",
		page_size=100,
	)
	response = client.describe_images(request)
	images = response.body.images.image if response.body and response.body.images else []
	ubuntu_images = [image for image in images if image.image_name and image.image_name.startswith("ubuntu_20_04")]
	if not ubuntu_images:
		raise RuntimeError("no Ubuntu 20.04 public image found")
	ubuntu_images.sort(key=lambda item: item.creation_time or "", reverse=True)
	return ubuntu_images[0].image_id


def find_existing_image(client: EcsClient, region_id: str, image_name: str) -> Optional[str]:
	request = ecs_models.DescribeImagesRequest(
		region_id=region_id,
		image_name=image_name,
		image_owner_alias="self",
	)
	response = client.describe_images(request)
	images = response.body.images.image if response.body and response.body.images else []
	for image in images:
		if image.image_name == image_name:
			return image.image_id
	return None


def _extract_instance_types(response: ecs_models.DescribeAvailableResourceResponse) -> list[str]:
	result: list[str] = []
	if not response.body or not response.body.available_zones:
		return result
	zones = response.body.available_zones.available_zone or []
	for zone in zones:
		resources = zone.available_resources.available_resource if zone.available_resources else []
		for resource in resources:
			if resource.type != "InstanceType":
				continue
			supported = resource.supported_resources.supported_resource if resource.supported_resources else []
			for item in supported:
				if item.status_category in {"WithStock", "ClosedWithStock"}:
					result.append(item.value)
	return result


def pick_instance_type(
	client: EcsClient,
	region_id: str,
	zone_id: str,
	requested_type: str,
	min_cpu_cores: int,
	min_memory_gb: float,
	spot_strategy: Optional[str],
) -> str:
	request = ecs_models.DescribeAvailableResourceRequest(
		region_id=region_id,
		zone_id=zone_id,
		destination_resource="InstanceType",
		resource_type="instance",
		instance_charge_type="PostPaid",
		spot_strategy=spot_strategy,
		instance_type=requested_type,
	)
	response = client.describe_available_resource(request)
	available = _extract_instance_types(response)
	if requested_type in available:
		return requested_type
	request = ecs_models.DescribeAvailableResourceRequest(
		region_id=region_id,
		zone_id=zone_id,
		destination_resource="InstanceType",
		resource_type="instance",
		instance_charge_type="PostPaid",
		spot_strategy=spot_strategy,
		cores=min_cpu_cores,
		memory=min_memory_gb,
	)
	response = client.describe_available_resource(request)
	available = _extract_instance_types(response)
	if not available:
		return requested_type
	return available[0]


def find_image_across_regions(credentials: AliCredentials, target_region_id: str, image_name: str) -> Optional[ImageSource]:
	seed_client = create_client(credentials, target_region_id)
	region_ids = list_regions(seed_client)
	for region_id in region_ids:
		if region_id == target_region_id:
			continue
		client = create_client(credentials, region_id)
		image_id = find_existing_image(client, region_id, image_name)
		if image_id:
			return ImageSource(region_id=region_id, image_id=image_id)
	return None


def wait_for_image_available(client: EcsClient, region_id: str, image_id: str, poll_interval: int, timeout: int) -> None:
	def is_available() -> bool:
		request = ecs_models.DescribeImagesRequest(region_id=region_id, image_id=image_id)
		response = client.describe_images(request)
		images = response.body.images.image if response.body and response.body.images else []
		if not images:
			return False
		status = images[0].status
		logger.info(f"image {image_id} status: {status}")
		if status in {"CreateFailed", "UnAvailable", "Deprecated"}:
			raise RuntimeError(f"image {image_id} failed with status: {status}")
		return status == "Available"

	wait_until(is_available, timeout=timeout, retry_interval=poll_interval)


def copy_image(credentials: AliCredentials, source: ImageSource, destination_region_id: str, destination_name: str) -> str:
	client = create_client(credentials, source.region_id)
	request = ecs_models.CopyImageRequest(
		region_id=source.region_id,
		destination_region_id=destination_region_id,
		image_id=source.image_id,
		destination_image_name=destination_name,
	)
	response = client.copy_image(request)
	if not response.body or not response.body.image_id:
		raise RuntimeError("failed to copy image")
	return response.body.image_id


async def prepare_instance(host: str, config: ImageBuildConfig) -> None:
	key_path = str(Path(config.ssh_private_key_path).expanduser())
	await wait_for_tcp_port_open(host, 22, timeout=config.wait_timeout, interval=3)
	async with asyncssh.connect(
		host,
		username=config.ssh_username,
		client_keys=[key_path],
		known_hosts=None,
	) as conn:
		commands = [
			"sudo apt-get update -y",
			(
				"sudo apt-get install -y "
				"build-essential clang cmake pkg-config libssl-dev git curl ca-certificates"
			),
			"sudo mkdir -p /opt/conflux/src",
			f"sudo mkdir -p {DEFAULT_REMOTE_CONFIG_DIR}",
			f"sudo chmod 755 {DEFAULT_REMOTE_CONFIG_DIR}",
			(
				"if [ ! -d /opt/conflux/src/conflux-rust ]; then "
				"sudo git clone --depth 1 https://github.com/Conflux-Chain/conflux-rust.git /opt/conflux/src/conflux-rust; "
				"fi"
			),
			(
				"sudo bash -lc 'set -e; "
				"cd /opt/conflux/src/conflux-rust; "
				f"git fetch --depth 1 origin {config.conflux_git_ref} || true; "
				f"git checkout {config.conflux_git_ref} || git checkout FETCH_HEAD; "
				"git submodule update --init --recursive; "
				"curl https://sh.rustup.rs -sSf | sh -s -- -y; "
				"source $HOME/.cargo/env; "
				"cargo --version; "
				"cargo build --release --bin conflux; "
				"install -m 0755 target/release/conflux /usr/local/bin/conflux'"
			),
			"sudo bash -lc 'echo \"conflux config will be injected later\" > /opt/conflux/config/README.txt'",
		]
		for cmd in commands:
			logger.info(f"remote: {cmd}")
			result = await conn.run(cmd, check=False)
			if result.stdout:
				logger.info(result.stdout.strip())
			if result.stderr:
				logger.warning(result.stderr.strip())
			if result.exit_status != 0:
				raise RuntimeError(f"remote command failed: {cmd}")


async def inject_conflux_config(
	host: str,
	local_config_path: str,
	ssh_username: str,
	ssh_private_key_path: str,
	remote_config_dir: str = DEFAULT_REMOTE_CONFIG_DIR,
) -> None:
	key_path = str(Path(ssh_private_key_path).expanduser())
	local_path = Path(local_config_path).expanduser()
	if not local_path.exists():
		raise FileNotFoundError(f"config path not found: {local_path}")
	async with asyncssh.connect(
		host,
		username=ssh_username,
		client_keys=[key_path],
		known_hosts=None,
	) as conn:
		await conn.run(f"sudo mkdir -p {remote_config_dir}", check=True)
		await asyncssh.scp(local_path, (conn, remote_config_dir), recursive=True)


def create_server_image(config: ImageBuildConfig, dry_run: bool = False) -> str:
	image_name = build_image_name(config.image_prefix, config.conflux_git_ref)
	client = create_client(config.credentials, config.region_id, config.endpoint)
	if not config.base_image_id:
		config = ImageBuildConfig(
			credentials=config.credentials,
			base_image_id=find_latest_ubuntu_image(client, config.region_id),
			instance_type=config.instance_type,
			v_switch_id=config.v_switch_id,
			security_group_id=config.security_group_id,
			key_pair_name=config.key_pair_name,
			conflux_git_ref=config.conflux_git_ref,
			region_id=config.region_id,
			zone_id=config.zone_id,
			endpoint=config.endpoint,
			image_prefix=config.image_prefix,
			instance_name_prefix=config.instance_name_prefix,
			internet_max_bandwidth_out=config.internet_max_bandwidth_out,
			ssh_username=config.ssh_username,
			ssh_private_key_path=config.ssh_private_key_path,
			poll_interval=config.poll_interval,
			wait_timeout=config.wait_timeout,
			cleanup_builder_instance=config.cleanup_builder_instance,
			search_all_regions=config.search_all_regions,
			use_spot=config.use_spot,
			spot_strategy=config.spot_strategy,
			vpc_name=config.vpc_name,
			vswitch_name=config.vswitch_name,
			security_group_name=config.security_group_name,
			vpc_cidr=config.vpc_cidr,
			vswitch_cidr=config.vswitch_cidr,
		)

	existing_image_id = find_existing_image(client, config.region_id, image_name)
	if existing_image_id:
		logger.info(f"image already exists in {config.region_id}: {existing_image_id}")
		if dry_run:
			return f"dry-run:{existing_image_id}"
		wait_for_image_available(client, config.region_id, existing_image_id, config.poll_interval, config.wait_timeout)
		return existing_image_id

	if config.search_all_regions:
		source = find_image_across_regions(config.credentials, config.region_id, image_name)
		if source:
			if dry_run:
				logger.info(
					"dry-run: would copy image %s from %s to %s",
					source.image_id,
					source.region_id,
					config.region_id,
				)
				return f"dry-run:copy:{source.image_id}"
			logger.info(f"copying image {source.image_id} from {source.region_id} to {config.region_id}")
			image_id = copy_image(config.credentials, source, config.region_id, image_name)
			logger.info(f"image copy started: {image_id}")
			wait_for_image_available(client, config.region_id, image_id, config.poll_interval, config.wait_timeout)
			return image_id

	if dry_run:
		logger.info(
			"dry-run: would create image %s using base image %s, vSwitch %s, security group %s",
			image_name,
			config.base_image_id,
			config.v_switch_id or config.vswitch_name,
			config.security_group_id or config.security_group_name,
		)
		return f"dry-run:{image_name}"

	memory_ranges = [(2.0, 2.0), (4.0, 4.0)]
	spot_strategy = config.spot_strategy if config.use_spot else None
	selection = None
	for min_mem, max_mem in memory_ranges:
		selection = pick_zone_and_instance_type(
			client,
			config.region_id,
			config.min_cpu_cores,
			min_mem,
			max_mem,
			spot_strategy,
			config.cpu_vendor,
		)
		if selection:
			break
	if not selection and config.use_spot:
		spot_strategy = None
		for min_mem, max_mem in memory_ranges:
			selection = pick_zone_and_instance_type(
				client,
				config.region_id,
				config.min_cpu_cores,
				min_mem,
				max_mem,
				spot_strategy,
				config.cpu_vendor,
			)
			if selection:
				break
	if not selection:
		raise RuntimeError("no in-stock instance type found")
	zone_id, selected_type, vendor = selection
	use_spot = spot_strategy is not None
	config = ImageBuildConfig(
		credentials=config.credentials,
		base_image_id=config.base_image_id,
		instance_type=selected_type,
		min_cpu_cores=config.min_cpu_cores,
		min_memory_gb=config.min_memory_gb,
		max_memory_gb=config.max_memory_gb,
		cpu_vendor=vendor or config.cpu_vendor,
		v_switch_id=config.v_switch_id,
		security_group_id=config.security_group_id,
		key_pair_name=config.key_pair_name,
		conflux_git_ref=config.conflux_git_ref,
		region_id=config.region_id,
		zone_id=zone_id,
		endpoint=config.endpoint,
		image_prefix=config.image_prefix,
		instance_name_prefix=config.instance_name_prefix,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		ssh_username=config.ssh_username,
		ssh_private_key_path=config.ssh_private_key_path,
		poll_interval=config.poll_interval,
		wait_timeout=config.wait_timeout,
		cleanup_builder_instance=config.cleanup_builder_instance,
		search_all_regions=config.search_all_regions,
		use_spot=use_spot,
		spot_strategy=config.spot_strategy,
		vpc_name=config.vpc_name,
		vswitch_name=config.vswitch_name,
		security_group_name=config.security_group_name,
		vpc_cidr=config.vpc_cidr,
		vswitch_cidr=config.vswitch_cidr,
	)
	config = ensure_network_resources(client, config)
	ensure_key_pair(client, config.region_id, config.key_pair_name, config.ssh_private_key_path)

	instance_id = ""
	try:
		instance_id = create_builder_instance(client, config)
		logger.info(f"builder instance created: {instance_id}")
		status = wait_instance_status(
			client,
			config.region_id,
			instance_id,
			["Stopped", "Running"],
			config.poll_interval,
			config.wait_timeout,
		)
		if status == "Stopped":
			start_instance(client, instance_id)
		wait_instance_status(
			client,
			config.region_id,
			instance_id,
			["Running"],
			config.poll_interval,
			config.wait_timeout,
		)
		allocate_public_ip(client, instance_id)
		public_ip = wait_instance_running(client, config.region_id, instance_id, config.poll_interval, config.wait_timeout)
		logger.info(f"builder instance ready: {public_ip}")

		asyncio.run(prepare_instance(public_ip, config))
		logger.info("stopping builder instance before image creation")
		stop_instance(client, instance_id, "StopCharging")
		wait_instance_status(
			client,
			config.region_id,
			instance_id,
			["Stopped"],
			config.poll_interval,
			config.wait_timeout,
		)

		create_request = ecs_models.CreateImageRequest(
			region_id=config.region_id,
			instance_id=instance_id,
			image_name=image_name,
		)
		create_response = client.create_image(create_request)
		if not create_response.body or not create_response.body.image_id:
			stop_instance(client, instance_id, None)
			wait_instance_status(
				client,
				config.region_id,
				instance_id,
				["Stopped"],
				config.poll_interval,
				config.wait_timeout,
			)
			create_response = client.create_image(create_request)
		if not create_response.body or not create_response.body.image_id:
			raise RuntimeError("failed to create image")
		image_id = create_response.body.image_id
		logger.info(f"image creation started: {image_id}")

		wait_for_image_available(client, config.region_id, image_id, config.poll_interval, config.wait_timeout)
		return image_id
	finally:
		if config.cleanup_builder_instance and instance_id:
			delete_instance(
				client,
				config.region_id,
				instance_id,
				poll_interval=config.poll_interval,
				timeout=config.wait_timeout,
			)
			logger.info(f"builder instance deleted: {instance_id}")


def build_arg_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Create Alibaba Cloud image for Conflux nodes")
	parser.add_argument("--conflux-git-ref", default=DEFAULT_CONFLUX_GIT_REF, help="Conflux git ref to build on the image")
	parser.add_argument("--base-image-id", default=None, help="Base system image ID (Ubuntu public image if empty)")
	parser.add_argument("--instance-type", default=None, help="ECS instance type (auto-pick if empty)")
	parser.add_argument("--min-cpu-cores", type=int, default=2)
	parser.add_argument("--min-memory-gb", type=float, default=4.0)
	parser.add_argument("--v-switch-id", default=None, help="VSwitch ID (auto-create if empty)")
	parser.add_argument("--security-group-id", default=None, help="Security group ID (auto-create if empty)")
	parser.add_argument("--key-pair-name", default=DEFAULT_KEYPAIR_NAME, help="Key pair name for SSH")
	parser.add_argument("--region-id", default=DEFAULT_REGION_ID)
	parser.add_argument("--zone-id", default=None)
	parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Custom ECS endpoint")
	parser.add_argument("--image-prefix", default="conflux")
	parser.add_argument("--ssh-username", default="root")
	parser.add_argument("--ssh-private-key", default=DEFAULT_SSH_PRIVATE_KEY)
	parser.add_argument("--internet-max-bandwidth-out", type=int, default=10)
	parser.add_argument("--poll-interval", type=int, default=5)
	parser.add_argument("--wait-timeout", type=int, default=1800)
	parser.add_argument("--search-all-regions", action="store_true")
	parser.add_argument("--spot", action="store_true")
	parser.add_argument("--spot-strategy", default="SpotAsPriceGo")
	parser.add_argument("--vpc-name", default=DEFAULT_VPC_NAME)
	parser.add_argument("--vswitch-name", default=DEFAULT_VSWITCH_NAME)
	parser.add_argument("--security-group-name", default=DEFAULT_SECURITY_GROUP_NAME)
	parser.add_argument("--vpc-cidr", default=DEFAULT_VPC_CIDR)
	parser.add_argument("--vswitch-cidr", default=DEFAULT_VSWITCH_CIDR)
	parser.add_argument("--no-cleanup", action="store_true")
	parser.add_argument("--dry-run", action="store_true")
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
	image_id = create_server_image(config, dry_run=args.dry_run)
	logger.info(f"image id: {image_id}")


if __name__ == "__main__":
	main()