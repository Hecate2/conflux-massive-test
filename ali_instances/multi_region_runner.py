"""Aliyun provisioning helpers based on instance-region.json."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from loguru import logger

from ali_instances.cleanup_resources import cleanup_all_regions
from ali_instances.config import AliCredentials, EcsConfig, client
from ali_instances.image_build import DEFAULT_IMAGE_NAME, find_img
from ali_instances.instance_prep import (
    ensure_keypair,
    find_zone_for_instance_type,
    list_zones_for_instance_type,
    provision_instance_with_type,
)
from remote_simulation.launch_conflux_node import HostSpec
from remote_simulation.port_allocation import (
    evm_rpc_port,
    evm_rpc_ws_port,
    p2p_port,
    pubsub_port,
    remote_rpc_port,
    rpc_port,
)


@dataclass
class AliTypeSpec:
    name: str
    nodes_per_host: int


@dataclass
class AliInstancePlan:
    instance_type: str
    nodes_per_host: int


CleanupTarget = Tuple[List[str], AliCredentials, str, str]


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text())


def load_hardware_defaults(path: Path) -> Dict[str, int]:
    data = json.loads(path.read_text())
    return {item["name"]: item["nodes"] for item in data}


def resolve_nodes_per_host(type_name: str, nodes: Optional[int], defaults: Dict[str, int]) -> int:
    if nodes is not None:
        return nodes
    return defaults.get(type_name, 1)


def resolve_aliyun_types(
    region_cfg: Dict,
    account_cfg: Dict,
    hardware_defaults: Dict[str, int],
) -> List[AliTypeSpec]:
    types_cfg = region_cfg.get("type") or account_cfg.get("type") or [{"name": "ecs.g8i.xlarge"}]
    specs: List[AliTypeSpec] = []
    for item in types_cfg:
        name = item["name"]
        raw_nodes = item.get("nodes")
        nodes = int(raw_nodes) if raw_nodes is not None else None
        nodes_per_host = resolve_nodes_per_host(name, nodes, hardware_defaults)
        specs.append(AliTypeSpec(name=name, nodes_per_host=nodes_per_host))
    return specs


def preferred_zones(region_cfg: Dict) -> Optional[List[str]]:
    zones_cfg = region_cfg.get("zones")
    if not zones_cfg:
        return None
    return [z["name"] for z in zones_cfg if "name" in z]


def plan_region_instances(
    *,
    region_client,
    region_name: str,
    count: int,
    type_specs: Sequence[AliTypeSpec],
    preferred: Optional[List[str]],
) -> List[AliInstancePlan]:
    remaining = count
    plans: List[AliInstancePlan] = []
    last_available: Optional[AliInstancePlan] = None

    for spec in type_specs:
        zone = find_zone_for_instance_type(region_client, region_name, spec.name, preferred)
        if not zone:
            logger.warning(f"instance type {spec.name} not available in {region_name}")
            continue
        plan = AliInstancePlan(instance_type=spec.name, nodes_per_host=spec.nodes_per_host)
        plans.append(plan)
        last_available = plan
        remaining -= spec.nodes_per_host
        if remaining <= 0:
            break

    if remaining > 0:
        if not last_available:
            raise RuntimeError(f"no available instance types in {region_name}")
        extra = math.ceil(remaining / last_available.nodes_per_host)
        plans.extend(
            [
                AliInstancePlan(
                    instance_type=last_available.instance_type,
                    nodes_per_host=last_available.nodes_per_host,
                )
                for _ in range(extra)
            ]
        )

    return plans


def ports_for_nodes(max_nodes_per_host: int) -> List[int]:
    ports: List[int] = [22]
    for i in range(max_nodes_per_host):
        ports.extend(
            [
                p2p_port(i),
                rpc_port(i),
                remote_rpc_port(i),
                pubsub_port(i),
                evm_rpc_port(i),
                evm_rpc_ws_port(i),
            ]
        )
    return sorted(set(ports))


def resolve_aliyun_credentials(cfg: Dict) -> AliCredentials:
    ak = cfg.get("access_key_id", "").strip()
    sk = cfg.get("access_key_secret", "").strip()
    if ak and sk:
        return AliCredentials(ak, sk)
    return EcsConfig().credentials


def provision_aliyun_hosts(
    *,
    config_path: Path,
    hardware_path: Path,
    common_tag: str = "conflux-massive-test",
) -> Tuple[List[HostSpec], List[CleanupTarget]]:
    config = load_json(config_path)
    hardware_defaults = load_hardware_defaults(hardware_path)

    aliyun_cfgs = config.get("aliyun", [])
    if not aliyun_cfgs:
        raise RuntimeError("missing aliyun config")

    hosts: List[HostSpec] = []
    cleanup_targets: List[CleanupTarget] = []

    for account_cfg in aliyun_cfgs:
        regions = account_cfg.get("regions", [])
        creds = resolve_aliyun_credentials(account_cfg)
        user_tag = account_cfg.get("user_tag", "chenxinghao")
        prefix = f"{common_tag}-{user_tag}"
        regions_used: List[str] = []

        for region_cfg in regions:
            region_name = region_cfg["name"]
            count = int(region_cfg.get("count", 0))
            if count <= 0:
                continue

            logger.info(f"准备在 {region_name} 启动 {count} 个节点")
            if region_name not in regions_used:
                regions_used.append(region_name)

            region_client = client(creds, region_name, None)
            type_specs = resolve_aliyun_types(region_cfg, account_cfg, hardware_defaults)
            preferred = preferred_zones(region_cfg)
            plans = plan_region_instances(
                region_client=region_client,
                region_name=region_name,
                count=count,
                type_specs=type_specs,
                preferred=preferred,
            )
            if not plans:
                raise RuntimeError(f"no instance plan generated for {region_name}")

            max_nodes_per_host = max(p.nodes_per_host for p in plans)
            ports = ports_for_nodes(max_nodes_per_host)

            cfg = EcsConfig(credentials=creds, region_id=region_name)
            cfg.ssh_username = "root"
            cfg.instance_name_prefix = prefix
            cfg.vpc_name = prefix
            cfg.vswitch_name = prefix
            cfg.security_group_name = prefix
            cfg.common_tag_key = common_tag
            cfg.common_tag_value = "true"
            cfg.user_tag_value = user_tag
            cfg.security_group_id = region_cfg.get("security_group_id") or account_cfg.get("security_group_id")
            zones_cfg = region_cfg.get("zones") or []
            if zones_cfg:
                cfg.zone_id = zones_cfg[0].get("name") or cfg.zone_id
                cfg.v_switch_id = zones_cfg[0].get("subnet") or cfg.v_switch_id

            ensure_keypair(region_client, region_name, cfg.key_pair_name, cfg.ssh_private_key_path)

            image_id = region_cfg.get("image") or account_cfg.get("image")
            if not image_id:
                existing = find_img(region_client, region_name, DEFAULT_IMAGE_NAME)
                if not existing and region_name == "ap-southeast-3":
                    existing = "m-8psi1b0lgs5qmakt4abt"
                if not existing:
                    raise RuntimeError(f"image {DEFAULT_IMAGE_NAME} not found in {region_name}")
                cfg.image_id = existing
            else:
                cfg.image_id = image_id

            def _no_stock(exc: Exception) -> bool:
                return "OperationDenied.NoStock" in str(exc)

            skipped_nodes = 0
            last_successful_type: Optional[str] = None
            last_successful_nodes: Optional[int] = None

            for plan in plans:
                zone_candidates = list_zones_for_instance_type(region_client, region_name, plan.instance_type, preferred)
                if not zone_candidates:
                    skipped_nodes += plan.nodes_per_host
                    continue

                created = False
                for zone_id in zone_candidates:
                    try:
                        handle = provision_instance_with_type(cfg, plan.instance_type, zone_id, ports)
                        hosts.append(
                            HostSpec(
                                ip=handle.public_ip,
                                nodes_per_host=plan.nodes_per_host,
                                ssh_user=cfg.ssh_username,
                                ssh_key_path=str(Path(cfg.ssh_private_key_path).expanduser()),
                                provider="aliyun",
                            )
                        )
                        last_successful_type = plan.instance_type
                        last_successful_nodes = plan.nodes_per_host
                        created = True
                        break
                    except Exception as exc:
                        if _no_stock(exc):
                            logger.warning(
                                f"no stock for {plan.instance_type} in {region_name}/{zone_id}, trying next zone"
                            )
                            continue
                        raise

                if not created:
                    logger.warning(f"no stock in any zone for {plan.instance_type} in {region_name}, skipping")
                    skipped_nodes += plan.nodes_per_host

            if skipped_nodes > 0:
                if not last_successful_type or not last_successful_nodes:
                    raise RuntimeError(f"no stock for all preferred types in {region_name}")
                extra = math.ceil(skipped_nodes / last_successful_nodes)
                zone_candidates = list_zones_for_instance_type(region_client, region_name, last_successful_type, preferred)
                if not zone_candidates:
                    raise RuntimeError(f"no zone available for {last_successful_type} in {region_name}")
                zone_id = zone_candidates[0]
                for _ in range(extra):
                    handle = provision_instance_with_type(cfg, last_successful_type, zone_id, ports)
                    hosts.append(
                        HostSpec(
                            ip=handle.public_ip,
                            nodes_per_host=last_successful_nodes,
                            ssh_user=cfg.ssh_username,
                            ssh_key_path=str(Path(cfg.ssh_private_key_path).expanduser()),
                            provider="aliyun",
                        )
                    )

        if regions_used:
            cleanup_targets.append((regions_used, creds, user_tag, prefix))

    return hosts, cleanup_targets


def cleanup_targets(targets: List[CleanupTarget], common_tag: str = "conflux-massive-test") -> None:
    for regions, creds, user_tag, prefix in targets:
        cleanup_all_regions(
            regions=regions,
            credentials=creds,
            common_tag=common_tag,
            user_tag=user_tag,
            name_prefix=prefix,
        )
