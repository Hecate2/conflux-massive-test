import time
from dataclasses import dataclass
from typing import Optional

from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from loguru import logger

from ali_instances.ecs_common import (
	AliCredentials,
	allocate_public_ip,
	create_client,
	delete_instance,
	ensure_network_resources,
	pick_system_disk_category,
	pick_zone_and_instance_type,
	start_instance,
	wait_instance_running,
	wait_instance_status,
) 


@dataclass(frozen=True)
class SingleInstanceConfig:
	credentials: AliCredentials
	image_id: str
	instance_type: Optional[str]
	v_switch_id: Optional[str]
	security_group_id: Optional[str]
	key_pair_name: str = "chenxinghao-conflux-image-builder"
	region_id: str = "ap-southeast-3"
	zone_id: Optional[str] = None
	endpoint: Optional[str] = None
	ssh_username: str = "root"
	ssh_private_key_path: str = "./keys/chenxinghao-conflux-image-builder.pem"
	instance_name_prefix: str = "conflux-massive-test-single-instance"
	internet_max_bandwidth_out: int = 10
	poll_interval: int = 5
	wait_timeout: int = 1800
	use_spot: bool = True
	spot_strategy: str = "SpotAsPriceGo"
	vpc_name: str = "conflux-image-builder"
	vswitch_name: str = "conflux-image-builder"
	security_group_name: str = "conflux-image-builder"
	vpc_cidr: str = "10.0.0.0/16"
	vswitch_cidr: str = "10.0.0.0/24"
	cpu_vendor: Optional[str] = None


@dataclass
class SingleInstanceHandle:
	client: EcsClient
	config: SingleInstanceConfig
	instance_id: str
	public_ip: str


def create_instance(client: EcsClient, config: SingleInstanceConfig) -> str:
	if not config.instance_type:
		raise ValueError("instance_type is required for instance creation")
	instance_name = f"{config.instance_name_prefix}-{int(time.time())}"
	system_disk_category = pick_system_disk_category(client, config.region_id, config.zone_id)
	system_disk = ecs_models.CreateInstanceRequestSystemDisk(category=system_disk_category, size=100) if system_disk_category else None
	request = ecs_models.CreateInstanceRequest(
		region_id=config.region_id,
		zone_id=config.zone_id,
		image_id=config.image_id,
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


def provision_single_instance(config: SingleInstanceConfig) -> SingleInstanceHandle:
	client = create_client(config.credentials, config.region_id, config.endpoint)
	selection = pick_zone_and_instance_type(
		client,
		config.region_id,
		min_cpu_cores=4,
		min_memory_gb=16.0,
		max_memory_gb=16.0,
		spot_strategy=config.spot_strategy if config.use_spot else None,
		cpu_vendor=config.cpu_vendor,
	)
	if not selection:
		raise RuntimeError("no in-stock instance type found")
	zone_id, instance_type, vendor = selection
	zone_id, vswitch_id, security_group_id = ensure_network_resources(
		client,
		region_id=config.region_id,
		zone_id=zone_id,
		v_switch_id=config.v_switch_id,
		security_group_id=config.security_group_id,
		vpc_name=config.vpc_name,
		vpc_cidr=config.vpc_cidr,
		vswitch_name=config.vswitch_name,
		vswitch_cidr=config.vswitch_cidr,
		security_group_name=config.security_group_name,
		security_group_desc="conflux single instance",
		open_ports=[],
	)
	updated_config = SingleInstanceConfig(
		credentials=config.credentials,
		image_id=config.image_id,
		instance_type=instance_type,
		v_switch_id=vswitch_id,
		security_group_id=security_group_id,
		key_pair_name=config.key_pair_name,
		region_id=config.region_id,
		zone_id=zone_id,
		endpoint=config.endpoint,
		ssh_username=config.ssh_username,
		ssh_private_key_path=config.ssh_private_key_path,
		instance_name_prefix=config.instance_name_prefix,
		internet_max_bandwidth_out=config.internet_max_bandwidth_out,
		poll_interval=config.poll_interval,
		wait_timeout=config.wait_timeout,
		use_spot=config.use_spot,
		spot_strategy=config.spot_strategy,
		vpc_name=config.vpc_name,
		vswitch_name=config.vswitch_name,
		security_group_name=config.security_group_name,
		vpc_cidr=config.vpc_cidr,
		vswitch_cidr=config.vswitch_cidr,
		cpu_vendor=vendor or config.cpu_vendor,
	)
	instance_id = create_instance(client, updated_config)
	logger.info(f"instance created: {instance_id}")
	status = wait_instance_status(
		client,
		updated_config.region_id,
		instance_id,
		["Stopped", "Running"],
		updated_config.poll_interval,
		updated_config.wait_timeout,
	)
	if status == "Stopped":
		start_instance(client, instance_id)
	allocate_public_ip(client, updated_config.region_id, instance_id, updated_config.poll_interval, updated_config.wait_timeout)
	public_ip = wait_instance_running(client, updated_config.region_id, instance_id, updated_config.poll_interval, updated_config.wait_timeout)
	logger.info(f"instance ready: {public_ip}")
	return SingleInstanceHandle(client=client, config=updated_config, instance_id=instance_id, public_ip=public_ip)


def cleanup_single_instance(handle: SingleInstanceHandle) -> None:
	delete_instance(
		handle.client,
		handle.config.region_id,
		handle.instance_id,
		handle.config.poll_interval,
		handle.config.wait_timeout,
	)
	logger.info(f"instance deleted: {handle.instance_id}")
