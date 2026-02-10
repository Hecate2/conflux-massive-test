# pyright: reportOptionalOperand=false

import json
import time
import traceback
import inspect
from typing import List, Tuple

from alibabacloud_ecs20140526.models import DescribeInstancesRequest, RunInstancesRequestTag, RunInstancesRequestSystemDisk, RunInstancesRequest, DescribeInstancesResponseBodyInstancesInstance, DeleteInstancesRequest
from loguru import logger

from cloud_provisioner.cleanup_instances.types import InstanceInfoWithTag

from ..create_instances.types import InstanceStatus, RegionInfo, ZoneInfo, InstanceType, CreateInstanceError
from ..create_instances.instance_config import InstanceConfig, DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE
from alibabacloud_ecs20140526.client import Client
    

def _instance_tags(cfg: InstanceConfig) -> List[RunInstancesRequestTag]:
    return [
        RunInstancesRequestTag(key=DEFAULT_COMMON_TAG_KEY, value=DEFAULT_COMMON_TAG_VALUE),
        RunInstancesRequestTag(key=cfg.user_tag_key, value=cfg.user_tag_value)
    ]
    
def as_instance_info_with_tag(rep: DescribeInstancesResponseBodyInstancesInstance):
    if rep.tags:
        tags = {tag.tag_key: tag.tag_value for tag in rep.tags.tag}
    else:
        tags = dict()
    return InstanceInfoWithTag(instance_id=rep.instance_id, instance_name=rep.instance_name, tags=tags) # pyright: ignore[reportArgumentType]
    

def create_instances_in_zone(
    client: Client,
    cfg: InstanceConfig,
    region_info: RegionInfo,
    zone_info: ZoneInfo,
    instance_type: InstanceType,
    max_amount: int,
    min_amount: int,
) -> Tuple[list[str], CreateInstanceError]:
    disk_size = cfg.disk_size or 20
    # Treat empty string as 'unspecified' so some instance types that don't support a forced
    # disk category (e.g., ecs.xn4.small) can be created by letting the cloud pick the default.
    disk_category = cfg.disk_category if cfg.disk_category not in (None, "") else None
    disk = RunInstancesRequestSystemDisk(size=str(disk_size))
    if disk_category:
        disk.category = disk_category
    name = f"{cfg.instance_name_prefix}-{int(time.time())}"
        
    # First try spot instances (aggressive pricing); if not enough or fails, fallback to NoSpot for the remaining
    req_spot = RunInstancesRequest(
        region_id=region_info.id,
        zone_id=zone_info.id,
        image_id=region_info.image_id,
        instance_type=instance_type.name,
        security_group_id=region_info.security_group_id,
        v_switch_id=zone_info.v_switch_id,
        key_pair_name=region_info.key_pair_name,
        instance_name=name,
        internet_max_bandwidth_out=cfg.internet_max_bandwidth_out,
        internet_charge_type="PayByTraffic",
        instance_charge_type="PostPaid",
        tag=_instance_tags(cfg),
        amount=max_amount,
        min_amount=min_amount,
        system_disk=disk,
        spot_strategy="SpotAsPriceGo",
    )

    if disk_size and disk_size > 0:
        req_spot.system_disk = disk

    if cfg.use_spot:
        req_spot.spot_strategy = cfg.spot_strategy or "SpotAsPriceGo"

    spot_ids = []
    spot_error_type = CreateInstanceError.Others

    try:
        resp = client.run_instances(req_spot)
        spot_ids = resp.body.instance_id_sets.instance_id_set or []
        assert spot_ids is not None
        logger.success(f"Create instances (spot) at {region_info.id}/{zone_info.id}: instance_type={instance_type.name}, amount={len(spot_ids)}, ids={spot_ids}")
    except Exception as exc:
        code = getattr(exc, "code", None)
        spot_error_type = CreateInstanceError.Others
        if code == "OperationDenied.NoStock":
            spot_error_type = CreateInstanceError.NoStock
            logger.warning(f"No spot stock for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={req_spot.min_amount}~{req_spot.amount}")
        elif code == "InvalidResourceType.NotSupported":
            spot_error_type = CreateInstanceError.NoInstanceType
            logger.warning(f"Spot not supported in {region_info.id}/{zone_info.id}, trying other zones... instance_type={instance_type.name}, amount={req_spot.min_amount}~{req_spot.amount}")
        else:
            logger.error(f"spot run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
            if code == "InvalidSystemDiskCategory.ValueNotSupported":
                try:
                    payload = req.to_map() if hasattr(req, "to_map") else {}
                    logger.error(f"debug SystemDisk payload: {payload.get('SystemDisk')}")
                except Exception:
                    pass
            logger.error(traceback.format_exc())

    # If spot created all requested instances, return success
    if len(spot_ids) == max_amount:
        return spot_ids, CreateInstanceError.Nil

    # Otherwise try to create remaining instances as NoSpot
    remaining = max_amount - len(spot_ids)
    if remaining <= 0:
        # Shouldn't happen, but guard anyway
        return spot_ids, CreateInstanceError.Nil

    # Compute min_amount required for fallback so that overall min_amount is satisfied
    needed_min_for_fallback = max(min_amount - len(spot_ids), 0)

    # Build a single NoSpot request object and set min_amount attribute when needed
    req_nospot = RunInstancesRequest(
        region_id=region_info.id,
        zone_id=zone_info.id,
        image_id=region_info.image_id,
        instance_type=instance_type.name,
        security_group_id=region_info.security_group_id,
        v_switch_id=zone_info.v_switch_id,
        key_pair_name=region_info.key_pair_name,
        instance_name=name,
        internet_max_bandwidth_out=cfg.internet_max_bandwidth_out,
        internet_charge_type="PayByTraffic",
        instance_charge_type="PostPaid",
        tag=_instance_tags(cfg),
        amount=remaining,
        system_disk=disk,
        spot_strategy="NoSpot",
    )

    if needed_min_for_fallback > 0:
        # Set attribute after construction to avoid duplicating constructor calls
        req_nospot.min_amount = needed_min_for_fallback

    nospot_ids = []
    nospot_error_type = CreateInstanceError.Others

    try:
        resp = client.run_instances(req_nospot)
        nospot_ids = resp.body.instance_id_sets.instance_id_set or []
        assert nospot_ids is not None
        logger.success(f"Create instances (nospot) at {region_info.id}/{zone_info.id}: instance_type={instance_type.name}, amount={len(nospot_ids)}, ids={nospot_ids}")
    except Exception as exc:
        code = getattr(exc, "code", None)
        nospot_error_type = CreateInstanceError.Others
        if code == "OperationDenied.NoStock":
            nospot_error_type = CreateInstanceError.NoStock
            logger.warning(f"No stock for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={getattr(req_nospot, 'min_amount', 0)}~{getattr(req_nospot, 'amount', remaining)}")
        elif code == "InvalidResourceType.NotSupported":
            nospot_error_type = CreateInstanceError.NoInstanceType
            logger.warning(f"Request not supported in {region_info.id}/{zone_info.id}, trying other zones... instance_type={instance_type.name}, amount={getattr(req_nospot, 'min_amount', 0)}~{getattr(req_nospot, 'amount', remaining)}")
        else:
            logger.error(f"nospot run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
            logger.error(traceback.format_exc())

    total_ids = (spot_ids or []) + (nospot_ids or [])

    if len(total_ids) > 0:
        if len(total_ids) < max_amount:
            logger.warning(f"Partially created instances at {region_info.id}/{zone_info.id}: requested={max_amount}, actual={len(total_ids)}")
        return total_ids, CreateInstanceError.Nil

    # If no instances were created, prefer the error from nospot if it ran, otherwise use spot error
    final_error = nospot_error_type if nospot_ids == [] else CreateInstanceError.Others
    if nospot_ids == [] and spot_ids == []:
        # choose the more specific error if available
        if nospot_error_type != CreateInstanceError.Others:
            final_error = nospot_error_type
        else:
            final_error = spot_error_type

    return [], final_error

# Save the count of lines of code for this function
CREATE_INSTANCES_IN_ZONE_LOC = len(inspect.getsource(create_instances_in_zone).splitlines())

def describe_instance_status(client: Client, region_id: str, instance_ids: List[str]):
    running_instances = dict()
    pending_instances = set()
    
    for i in range(0, len(instance_ids), 100):
        query_chunk = instance_ids[i: i+100]
        
        rep = client.describe_instances(DescribeInstancesRequest(
            region_id=region_id, page_size=100, instance_ids=json.dumps(query_chunk)))
        instance_status = rep.body.instances.instance

        for instance in instance_status:
            if instance.status not in ["Running"]:
                continue
            public_ip = instance.public_ip_address.ip_address[0]
            private_ip = instance.vpc_attributes.private_ip_address.ip_address[0] or instance.inner_ip_address.ip_address[0]
            running_instances[instance.instance_id] = (public_ip, private_ip)
        
        # 阿里云启动阶段也可能读到 instance 是 stopped 的状态
        pending_instances.update({i.instance_id for i in instance_status if i.status in [
                                 "Starting", "Pending", "Stopped"]})
        time.sleep(0.5)
    return InstanceStatus(running_instances=running_instances, pending_instances=pending_instances)


def get_instances_with_tag(client: Client, region_id: str) -> List[InstanceInfoWithTag]:
    instances = []
    page_number = 1
    
    while True:
        rep = client.describe_instances(DescribeInstancesRequest(region_id=region_id, page_number=page_number, page_size=50))
        instances.extend([as_instance_info_with_tag(instance) for instance in rep.body.instances.instance])
        
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    return instances

def delete_instances(client: Client, region_id: str, instances_ids: List[str]):
    for i in range(0, len(instances_ids), 100):
        chunks = instances_ids[i:i+100]
        while True: 
            try: 
                client.delete_instances(DeleteInstancesRequest(region_id = region_id, force_stop=True, force=True, instance_id=chunks))
                break
            except Exception as e:
                code = getattr(e, "code", None)
                if code == "IncorrectInstanceStatus.Initializing":
                    logger.warning(f"Some instances in region {region_id} is still initializing, waiting_retry")
            time.sleep(5)