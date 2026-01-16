import asyncio
import ipaddress
import json
import os
import socket
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Sequence
from alibabacloud_ecs20140526.client import Client as EcsClient
import asyncssh
import subprocess
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_openapi.models import Config as AliyunConfig
from dotenv import load_dotenv
from loguru import logger
from utils.wait_until import wait_until
from .image_config import ImageBuildConfig

DEFAULT_ENDPOINT = "cloudcontrol.aliyuncs.com"


@dataclass(frozen=True)
class AliCredentials:
	access_key_id: str
	access_key_secret: str


def load_ali_credentials() -> AliCredentials:
	load_dotenv()
	access_key_id = os.getenv("ALI_ACCESS_KEY_ID", "").strip()
	access_key_secret = os.getenv("ALI_ACCESS_KEY_SECRET", "").strip()
	if not access_key_id or not access_key_secret:
		raise ValueError("Missing ALI_ACCESS_KEY_ID or ALI_ACCESS_KEY_SECRET in .env")
	return AliCredentials(access_key_id=access_key_id, access_key_secret=access_key_secret)


def load_endpoint() -> Optional[str]:
	value = os.getenv("ALI_ECS_ENDPOINT", "").strip()
	return value or DEFAULT_ENDPOINT


def normalize_endpoint(region_id: str, endpoint: Optional[str]) -> Optional[str]:
	if not endpoint:
		return None
	if "cloudcontrol.aliyuncs.com" in endpoint:
		return f"ecs.{region_id}.aliyuncs.com"
	return endpoint


def create_client(credentials: AliCredentials, region_id: str, endpoint: Optional[str] = None) -> EcsClient:
	endpoint = normalize_endpoint(region_id, endpoint)
	config = AliyunConfig(
		access_key_id=credentials.access_key_id,
		access_key_secret=credentials.access_key_secret,
		region_id=region_id,
		endpoint=endpoint,
	)
	return EcsClient(config)


def pick_zone_id(client: EcsClient, region_id: str) -> str:
	request = ecs_models.DescribeZonesRequest(region_id=region_id)
	response = client.describe_zones(request)
	zones = response.body.zones.zone if response.body and response.body.zones else []
	if not zones:
		raise RuntimeError(f"no zones available in region {region_id}")
	return zones[0].zone_id


def pick_system_disk_category(client: EcsClient, region_id: str, zone_id: str) -> Optional[str]:
	request = ecs_models.DescribeZonesRequest(region_id=region_id)
	response = client.describe_zones(request)
	zones = response.body.zones.zone if response.body and response.body.zones else []
	for zone in zones:
		if zone.zone_id != zone_id:
			continue
		available = zone.available_resources.resources_info if zone.available_resources else []
		for info in available:
			categories = info.system_disk_categories.supported_system_disk_category if info.system_disk_categories else []
			if categories:
				for preferred in ["cloud_essd", "cloud_ssd", "cloud_efficiency", "cloud"]:
					if preferred in categories:
						return preferred
				return categories[0]
	return None


def wait_instance_status(
	client: EcsClient,
	region_id: str,
	instance_id: str,
	desired_statuses: Sequence[str],
	poll_interval: int,
	timeout: int,
) -> str:
	status_holder: dict[str, Optional[str]] = {"status": None}

	def has_status() -> bool:
		status_holder["status"] = get_instance_status(client, region_id, instance_id)
		return status_holder["status"] in desired_statuses

	wait_until(has_status, timeout=timeout, retry_interval=poll_interval)
	return status_holder["status"] or ""


def get_instance_public_ip(client: EcsClient, region_id: str, instance_id: str) -> Optional[str]:
	request = ecs_models.DescribeInstancesRequest(
		region_id=region_id,
		instance_ids=json.dumps([instance_id]),
	)
	response = client.describe_instances(request)
	instances = response.body.instances.instance if response.body and response.body.instances else []
	if not instances:
		return None
	public_ips = instances[0].public_ip_address.ip_address if instances[0].public_ip_address else []
	return public_ips[0] if public_ips else None


def get_instance_status(client: EcsClient, region_id: str, instance_id: str) -> Optional[str]:
	request = ecs_models.DescribeInstancesRequest(
		region_id=region_id,
		instance_ids=json.dumps([instance_id]),
	)
	response = client.describe_instances(request)
	instances = response.body.instances.instance if response.body and response.body.instances else []
	return instances[0].status if instances else None


def wait_instance_running(client: EcsClient, region_id: str, instance_id: str, poll_interval: int, timeout: int) -> str:
	public_ip_holder: dict[str, Optional[str]] = {"ip": None}

	def is_ready() -> bool:
		status = get_instance_status(client, region_id, instance_id)
		public_ip_holder["ip"] = get_instance_public_ip(client, region_id, instance_id)
		logger.info(f"instance {instance_id} status: {status}, public_ip: {public_ip_holder['ip']}")
		return status == "Running" and bool(public_ip_holder["ip"])

	wait_until(is_ready, timeout=timeout, retry_interval=poll_interval)
	return public_ip_holder["ip"] or ""


async def wait_for_ssh_ready(
	host: str,
	username: str,
	private_key_path: str,
	timeout: int,
	interval: int = 3,
) -> None:
	await wait_for_tcp_port_open(host, 22, timeout=timeout, interval=interval)
	key_path = str(Path(private_key_path).expanduser())
	deadline = time.time() + timeout
	while time.time() < deadline:
		results = await asyncio.gather(
			asyncssh.connect(
				host,
				username=username,
				client_keys=[key_path],
				known_hosts=None,
			),
			return_exceptions=True,
		)
		conn = results[0]
		if isinstance(conn, Exception):
			await asyncio.sleep(interval)
			continue
		conn.close()
		await conn.wait_closed()
		return
	raise TimeoutError(f"SSH not ready for {host} after {timeout}s")


def start_instance(client: EcsClient, instance_id: str) -> None:
	request = ecs_models.StartInstanceRequest(instance_id=instance_id)
	client.start_instance(request)


def stop_instance(client: EcsClient, instance_id: str, stopped_mode: Optional[str] = None) -> None:
	request = ecs_models.StopInstanceRequest(
		instance_id=instance_id,
		force_stop=True,
		stopped_mode=stopped_mode,
	)
	client.stop_instance(request)


def allocate_public_ip(
	client: EcsClient,
	region_id: str,
	instance_id: str,
	poll_interval: int = 3,
	timeout: int = 120,
) -> Optional[str]:
	wait_instance_status(client, region_id, instance_id, ["Running", "Stopped"], poll_interval, timeout)
	request = ecs_models.AllocatePublicIpAddressRequest(instance_id=instance_id)
	response = client.allocate_public_ip_address(request)
	return response.body.ip_address if response.body else None


def delete_instance(
	client: EcsClient,
	region_id: str,
	instance_id: str,
	poll_interval: int = 5,
	timeout: int = 300,
) -> None:
	status = get_instance_status(client, region_id, instance_id)
	if not status:
		return
	if status != "Stopped":
		request = ecs_models.StopInstanceRequest(instance_id=instance_id, force_stop=True, stopped_mode="StopCharging")
		client.stop_instance(request)
		wait_instance_status(client, region_id, instance_id, ["Stopped"], poll_interval, timeout)
	delete_request = ecs_models.DeleteInstanceRequest(instance_id=instance_id, force=True, force_stop=True)
	client.delete_instance(delete_request)


def wait_vpc_available(client: EcsClient, region_id: str, vpc_id: str, timeout: int = 120) -> None:
	def is_available() -> bool:
		request = ecs_models.DescribeVpcsRequest(region_id=region_id, vpc_id=vpc_id)
		response = client.describe_vpcs(request)
		vpcs = response.body.vpcs.vpc if response.body and response.body.vpcs else []
		status = vpcs[0].status if vpcs else None
		return status == "Available"

	wait_until(is_available, timeout=timeout, retry_interval=3)


def ensure_vpc(client: EcsClient, region_id: str, vpc_name: str, cidr_block: str) -> str:
	request = ecs_models.DescribeVpcsRequest(region_id=region_id, page_size=50)
	response = client.describe_vpcs(request)
	vpcs = response.body.vpcs.vpc if response.body and response.body.vpcs else []
	for vpc in vpcs:
		if vpc.vpc_name == vpc_name:
			return vpc.vpc_id
	create_request = ecs_models.CreateVpcRequest(region_id=region_id, vpc_name=vpc_name, cidr_block=cidr_block)
	create_response = client.create_vpc(create_request)
	if not create_response.body or not create_response.body.vpc_id:
		raise RuntimeError("failed to create VPC")
	vpc_id = create_response.body.vpc_id
	wait_vpc_available(client, region_id, vpc_id)
	return vpc_id


def wait_vswitch_available(client: EcsClient, region_id: str, vswitch_id: str, timeout: int = 120) -> None:
	def is_available() -> bool:
		request = ecs_models.DescribeVSwitchesRequest(region_id=region_id, v_switch_id=vswitch_id)
		response = client.describe_vswitches(request)
		vswitches = response.body.v_switches.v_switch if response.body and response.body.v_switches else []
		status = vswitches[0].status if vswitches else None
		return status == "Available"

	wait_until(is_available, timeout=timeout, retry_interval=3)


def pick_available_vswitch_cidr(existing_cidrs: list[str], vpc_cidr: str) -> str:
	vpc_net = ipaddress.ip_network(vpc_cidr)
	used = {ipaddress.ip_network(cidr) for cidr in existing_cidrs if cidr}
	for subnet in vpc_net.subnets(new_prefix=24):
		if all(not subnet.overlaps(u) for u in used):
			return str(subnet)
	raise RuntimeError("no available /24 CIDR in VPC")


def _match_cpu_vendor(model: Optional[str], vendor: Optional[str]) -> bool:
	"""Return True if vendor is not specified or vendor substring is in model string."""
	if not vendor:
		return True
	if not model:
		return False
	vendor_lower = vendor.lower()
	model_lower = model.lower()
	return vendor_lower in model_lower


def _load_instance_type_map(client: EcsClient, instance_type_ids: list[str]) -> dict[str, ecs_models.DescribeInstanceTypesResponseBodyInstanceTypesInstanceType]:
	"""Load instance type descriptions and return a map of id -> instance type object."""
	if not instance_type_ids:
		return {}
	request = ecs_models.DescribeInstanceTypesRequest(instance_types=instance_type_ids)
	response = client.describe_instance_types(request)
	items = response.body.instance_types.instance_type if response.body and response.body.instance_types else []
	return {item.instance_type_id: item for item in items if item.instance_type_id} 


def ensure_vswitch(client: EcsClient, region_id: str, vpc_id: str, zone_id: str, name: str, cidr_block: str, vpc_cidr: str) -> str:
	request = ecs_models.DescribeVSwitchesRequest(region_id=region_id, vpc_id=vpc_id, page_size=50)
	response = client.describe_vswitches(request)
	vswitches = response.body.v_switches.v_switch if response.body and response.body.v_switches else []
	for vswitch in vswitches:
		if vswitch.v_switch_name == name and vswitch.zone_id == zone_id:
			return vswitch.v_switch_id
	existing_cidrs = [v.cidr_block for v in vswitches if v.cidr_block]
	if _cidr_overlaps(cidr_block, existing_cidrs):
		cidr_block = pick_available_vswitch_cidr(existing_cidrs, vpc_cidr)
	create_request = ecs_models.CreateVSwitchRequest(
		region_id=region_id,
		vpc_id=vpc_id,
		zone_id=zone_id,
		v_switch_name=name,
		cidr_block=cidr_block,
	)
	create_response = client.create_vswitch(create_request)
	if not create_response.body or not create_response.body.v_switch_id:
		raise RuntimeError("failed to create VSwitch")
	vswitch_id = create_response.body.v_switch_id
	wait_vswitch_available(client, region_id, vswitch_id)
	return vswitch_id


def ensure_security_group(client: EcsClient, region_id: str, vpc_id: str, name: str, description: str) -> str:
	request = ecs_models.DescribeSecurityGroupsRequest(region_id=region_id, vpc_id=vpc_id, page_size=50)
	response = client.describe_security_groups(request)
	groups = response.body.security_groups.security_group if response.body and response.body.security_groups else []
	for group in groups:
		if group.security_group_name == name:
			return group.security_group_id
	create_request = ecs_models.CreateSecurityGroupRequest(
		region_id=region_id,
		vpc_id=vpc_id,
		security_group_name=name,
		description=description,
	)
	create_response = client.create_security_group(create_request)
	if not create_response.body or not create_response.body.security_group_id:
		raise RuntimeError("failed to create security group")
	return create_response.body.security_group_id


def authorize_security_group_port(client: EcsClient, region_id: str, security_group_id: str, port: int) -> None:
	if security_group_allows_port(client, region_id, security_group_id, port):
		return
	request = ecs_models.AuthorizeSecurityGroupRequest(
		region_id=region_id,
		security_group_id=security_group_id,
		ip_protocol="tcp",
		port_range=f"{port}/{port}",
		source_cidr_ip="0.0.0.0/0",
	)
	client.authorize_security_group(request)


def ensure_network_resources(
	client: EcsClient,
	*,
	region_id: str,
	zone_id: Optional[str],
	v_switch_id: Optional[str],
	security_group_id: Optional[str],
	vpc_name: str,
	vpc_cidr: str,
	vswitch_name: str,
	vswitch_cidr: str,
	security_group_name: str,
	security_group_desc: str,
	open_ports: Sequence[int] = (),
) -> tuple[str, str, str]:
	selected_zone_id = zone_id or pick_zone_id(client, region_id)
	vpc_id = ensure_vpc(client, region_id, vpc_name, vpc_cidr)
	selected_vswitch_id = v_switch_id or ensure_vswitch(client, region_id, vpc_id, selected_zone_id, vswitch_name, vswitch_cidr, vpc_cidr)
	selected_security_group_id = security_group_id or ensure_security_group(client, region_id, vpc_id, security_group_name, security_group_desc)
	authorize_security_group_port(client, region_id, selected_security_group_id, 22)
	for port in open_ports:
		authorize_security_group_port(client, region_id, selected_security_group_id, port)
	return selected_zone_id, selected_vswitch_id, selected_security_group_id


def security_group_allows_port(client: EcsClient, region_id: str, security_group_id: str, port: int) -> bool:
	request = ecs_models.DescribeSecurityGroupAttributeRequest(
		region_id=region_id,
		security_group_id=security_group_id,
	)
	response = client.describe_security_group_attribute(request)
	permissions = response.body.permissions.permission if response.body and response.body.permissions else []
	port_range = f"{port}/{port}"
	for perm in permissions:
		if perm.ip_protocol != "tcp":
			continue
		if perm.port_range == port_range and perm.source_cidr_ip == "0.0.0.0/0":
			return True
	return False


def _cidr_overlaps(cidr_block: str, existing_cidrs: Sequence[str]) -> bool:
	if not cidr_block:
		return True
	new_net = ipaddress.ip_network(cidr_block)
	for cidr in existing_cidrs:
		if not cidr:
			continue
		if new_net.overlaps(ipaddress.ip_network(cidr)):
			return True
	return False


def vswitch_exists(client: EcsClient, region_id: str, vswitch_id: Optional[str]) -> bool:
	if not vswitch_id:
		return False
	request = ecs_models.DescribeVSwitchesRequest(region_id=region_id, v_switch_id=vswitch_id)
	response = client.describe_vswitches(request)
	vswitches = response.body.v_switches.v_switch if response.body and response.body.v_switches else []
	return any(v.v_switch_id == vswitch_id for v in vswitches if v.v_switch_id)


def security_group_exists(client: EcsClient, region_id: str, security_group_id: Optional[str]) -> bool:
	if not security_group_id:
		return False
	request = ecs_models.DescribeSecurityGroupsRequest(region_id=region_id, security_group_id=security_group_id)
	response = client.describe_security_groups(request)
	groups = response.body.security_groups.security_group if response.body and response.body.security_groups else []
	return any(group.security_group_id == security_group_id for group in groups if group.security_group_id)


def ensure_key_pair(client: EcsClient, region_id: str, key_pair_name: str, private_key_path: str) -> None:
	request = ecs_models.DescribeKeyPairsRequest(region_id=region_id, key_pair_name=key_pair_name)
	response = client.describe_key_pairs(request)
	keys = response.body.key_pairs.key_pair if response.body and response.body.key_pairs else []
	if keys:
		return
	key_path = ensure_private_key(private_key_path)
	result = subprocess.run(["ssh-keygen", "-y", "-f", key_path], capture_output=True, text=True, check=True)
	public_key = result.stdout.strip()
	import_request = ecs_models.ImportKeyPairRequest(
		region_id=region_id,
		key_pair_name=key_pair_name,
		public_key_body=public_key,
	)
	client.import_key_pair(import_request)


def ensure_private_key(private_key_path: str) -> str:
	key_path = Path(private_key_path).expanduser().resolve()
	if key_path.exists():
		result = subprocess.run(["ssh-keygen", "-y", "-f", str(key_path)], capture_output=True, text=True)
		if result.returncode == 0 and result.stdout.strip():
			return str(key_path)
		key_path.unlink()
	key_path.parent.mkdir(parents=True, exist_ok=True)
	result = subprocess.run(
		[
			"ssh-keygen",
			"-t",
			"rsa",
			"-b",
			"2048",
			"-m",
			"PEM",
			"-f",
			str(key_path),
			"-N",
			"",
		],
		capture_output=True,
		text=True,
		check=True,
	)
	if result.stderr:
		logger.warning(result.stderr.strip())
	return str(key_path)


def create_builder_instance(client: EcsClient, config: ImageBuildConfig) -> str:
	if not config.instance_type:
		raise ValueError("instance_type is required for builder instance creation")
	system_disk_category = pick_system_disk_category(client, config.region_id, config.zone_id)
	system_disk = ecs_models.CreateInstanceRequestSystemDisk(category=system_disk_category, size=40) if system_disk_category else None
	instance_name = f"{config.instance_name_prefix}-{int(time.time())}"
	if not vswitch_exists(client, config.region_id, config.v_switch_id) or not security_group_exists(
		client, config.region_id, config.security_group_id
	):
		selected_zone_id = config.zone_id or pick_zone_id(client, config.region_id)
		vpc_id = ensure_vpc(client, config.region_id, config.vpc_name)
		v_switch_id = config.v_switch_id or ensure_vswitch(
			client,
			config.region_id,
			vpc_id,
			selected_zone_id,
			config.vswitch_name,
			config.vswitch_cidr,
			config.vpc_cidr,
		)
		security_group_id = config.security_group_id or ensure_security_group(
			client, config.region_id, vpc_id, config.security_group_name, "conflux image builder"
		)
		# Update config with resolved networking attributes using the canonical ImageBuildConfig
		config = ImageBuildConfig(
			credentials=config.credentials,
			base_image_id=config.base_image_id,
			instance_type=config.instance_type,
			min_cpu_cores=config.min_cpu_cores,
			min_memory_gb=config.min_memory_gb,
			max_memory_gb=config.max_memory_gb,
			cpu_vendor=config.cpu_vendor,
			v_switch_id=v_switch_id,
			security_group_id=security_group_id,
			key_pair_name=config.key_pair_name,
			conflux_git_ref=config.conflux_git_ref,
			region_id=config.region_id,
			zone_id=selected_zone_id,
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
	request = ecs_models.CreateInstanceRequest(
		region_id=config.region_id,
		zone_id=config.zone_id,
		image_id=config.base_image_id,
		instance_type=config.instance_type,
		security_group_id=config.security_group_id,
		v_switch_id=config.v_switch_id,
		key_pair_name=config.key_pair_name,
		instance_name=instance_name,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		internet_charge_type="PayByTraffic",
		instance_charge_type="PostPaid",
		spot_strategy=config.spot_strategy if config.use_spot else None,
		system_disk=system_disk,
	)
	response = client.create_instance(request)
	if not response.body or not response.body.instance_id:
		raise RuntimeError("failed to create instance")
	return response.body.instance_id



async def wait_for_tcp_port_open(host: str, port: int, timeout: int, interval: int = 3) -> None:
	deadline = time.time() + timeout
	while time.time() < deadline:
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.settimeout(2)
			if sock.connect_ex((host, port)) == 0:
				return
		await asyncio.sleep(interval)
	raise TimeoutError(f"TCP port {port} not open on {host} within {timeout}s")
