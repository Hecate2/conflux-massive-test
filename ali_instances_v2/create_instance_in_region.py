from itertools import product
import math
import time
import traceback
import threading
from typing import List


from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from loguru import logger

from host_spec import HostSpec

from .region_create_manager import RegionCreateManager

from .types import RegionInfo, ZoneInfo, InstanceConfig, InstanceType
    
            
    
def create_instances_in_region(c: EcsClient, cfg: InstanceConfig, region_info: RegionInfo, instance_types: List[InstanceType], nodes: int):
    mgr = RegionCreateManager(region_info.id, nodes)
    thread1 = threading.Thread(target=mgr.describe_instances_loop, args=(c,))
    thread1.start()
    thread2 = threading.Thread(target=mgr.wait_for_ssh_loop)
    thread2.start()

    
    # TODO: 使用 stock 询价方式确定 default type?
    default_instance_type = instance_types[0]
    amount = math.ceil(nodes / default_instance_type.nodes)
    _try_create_in_single_zone(c, mgr, cfg, region_info, default_instance_type, amount)
    
    
    # 排列组合所有区域，可以在这里配置更复杂的尝试策略
    zone_plan = product(instance_types, region_info.zones.values())
    
    instance_type, zone_info = next(zone_plan)
    
    while True:
        rest_nodes = mgr.get_rest_nodes()
        if rest_nodes <= 0:
            logger.success(f"Region {region_info.id} launch complete")
            return _make_host_spec(mgr, region_info)
            
        instance_ids = _create_instances_in_zone(c, cfg, region_info, zone_info, instance_type, amount, allow_partial_success=True)
        if len(instance_ids) < amount:
            # 当前实例组合可用已经耗尽，尝试下一组
            try:
                instance_type, zone_info = next(zone_plan)
            except StopIteration:
                # 全部实例组合耗尽
                break
    
    # 如果全部实例组合耗尽，会到达这里
    rest_nodes = mgr.get_rest_nodes(wait_for_pendings=True)
    if rest_nodes > 0:
        logger.error(f"Cannot launch enough nodes, request {nodes}, actual {mgr.ready_nodes}")
        
    return _make_host_spec(mgr, region_info)
            
def _make_host_spec(mgr: RegionCreateManager, region_info: RegionInfo):
    ready_instances = mgr.copy_ready_instances()
    return [HostSpec(ip=ip, 
                     nodes_per_host=instance.type.nodes, 
                     ssh_user="root", 
                     ssh_key_path=region_info.key_path, 
                     provider = "aliyun",
                     region=region_info.id, 
                     instance_id=instance.instance_id)
            for (instance, ip) in ready_instances]
    
def _try_create_in_single_zone(c: EcsClient, mgr: RegionCreateManager, cfg: InstanceConfig, region_info: RegionInfo, instance_type: InstanceType, amount: int):
    for zone_info in region_info.zones.values():
        ids = _create_instances_in_zone(c, cfg, region_info, zone_info, instance_type, amount)
        if len(ids) == 0:
            continue
        elif len(ids) < amount:
            # TODO: 关闭部分成功的 instance?
            logger.warning(f"Only partial create instance success, even if minimum required ({region_info.id}/{zone_info.id})")
        else:
            mgr.submit_pending_instances(ids, instance_type)
            # 无论这些实例是否都成功，不会再走 create_in_single_zone 的逻辑
            return
        
    

def _create_instances_in_zone(
    c: EcsClient,
    cfg: InstanceConfig,
    region_info: RegionInfo,
    zone_info: ZoneInfo,
    instance_type: InstanceType,
    amount: int,
    allow_partial_success: bool = False,
) -> list[str]:
    disk = ecs_models.RunInstancesRequestSystemDisk(category="cloud_essd", size=str(cfg.disk_size))
    name = f"{cfg.instance_name_prefix}-{int(time.time())}"
        
    req = ecs_models.RunInstancesRequest(
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
        tag=cfg.instance_tags,
        amount=amount,
        system_disk=disk,
    )
    
    if allow_partial_success:
        req.min_amount = 1

    try:
        resp = c.run_instances(req)
        ids = resp.body.instance_id_sets.instance_id_set
        assert ids is not None
        logger.success(f"Create instances at {region_info.id}/{zone_info.id}: instance_type={instance_type.name}, amount={len(ids)}, ids={ids}")
        # ids = resp.body.instance_id_sets.instance_id_set if resp.body and resp.body.instance_id_sets else []
        return ids
    except Exception as exc:
        e = traceback.format_exc()
        code = getattr(exc, "code", None)
        if code == "OperationDenied.NoStock":
            logger.warning(f"No stock for {region_info.id}/{zone_info.id}, instance_type={instance_type.name}, amount={amount}")
            return []
        logger.error(f"run_instances failed for {region_info.id}/{zone_info.id}: {exc}")
        logger.error(e)
        return []