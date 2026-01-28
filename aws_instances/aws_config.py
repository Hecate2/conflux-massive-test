"""AWS config loader for instance-region.json."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclass
class AwsTypeSpec:
    name: str
    nodes_per_host: int


@dataclass
class AwsRegionPlan:
    region_name: str
    node_count: int
    image_id: str
    security_group_id: Optional[str]
    subnet_ids: List[str]
    type_specs: List[AwsTypeSpec]
    access_key_id: Optional[str]
    access_key_secret: Optional[str]
    user_tag: Optional[str]


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text())


def load_hardware_defaults(path: Path) -> Dict[str, int]:
    data = json.loads(path.read_text())
    return {item["name"]: int(item["nodes"]) for item in data}


def resolve_nodes_per_host(type_name: str, nodes: Optional[int], defaults: Dict[str, int]) -> int:
    if nodes is not None:
        return int(nodes)
    return int(defaults.get(type_name, 1))


def resolve_aws_types(
    region_cfg: Dict,
    account_cfg: Dict,
    hardware_defaults: Dict[str, int],
) -> List[AwsTypeSpec]:
    types_cfg = region_cfg.get("type") or account_cfg.get("type") or [{"name": "m6i.2xlarge"}]
    specs: List[AwsTypeSpec] = []
    for item in types_cfg:
        name = item["name"]
        raw_nodes = item.get("nodes")
        nodes = int(raw_nodes) if raw_nodes is not None else None
        nodes_per_host = resolve_nodes_per_host(name, nodes, hardware_defaults)
        specs.append(AwsTypeSpec(name=name, nodes_per_host=nodes_per_host))
    return specs


def active_regions(regions: Iterable[Dict]) -> List[Dict]:
    return [r for r in regions if int(r.get("count", 0)) > 0]


def collect_subnet_ids(region_cfg: Dict) -> List[str]:
    zones_cfg = region_cfg.get("zones") or []
    subnet_ids: List[str] = []
    for z in zones_cfg:
        subnet = z.get("subnet")
        if subnet:
            subnet_ids.append(str(subnet))
    return subnet_ids


def load_region_plans(config_path: Path, hardware_path: Path) -> List[AwsRegionPlan]:
    config = load_json(config_path)
    hardware_defaults = load_hardware_defaults(hardware_path)
    plans: List[AwsRegionPlan] = []
    accounts = config.get("aws") or []
    for account_cfg in accounts:
        regions_cfg = active_regions(account_cfg.get("regions") or [])
        for region_cfg in regions_cfg:
            region_name = region_cfg["name"]
            node_count = int(region_cfg.get("count", 0))
            if node_count <= 0:
                continue
            image_id = region_cfg.get("image") or account_cfg.get("image")
            if not image_id:
                raise ValueError(f"Missing image id for region {region_name}")
            security_group_id = region_cfg.get("security_group_id") or account_cfg.get("security_group_id")
            subnet_ids = collect_subnet_ids(region_cfg)
            type_specs = resolve_aws_types(region_cfg, account_cfg, hardware_defaults)
            plans.append(
                AwsRegionPlan(
                    region_name=region_name,
                    node_count=node_count,
                    image_id=image_id,
                    security_group_id=security_group_id,
                    subnet_ids=subnet_ids,
                    type_specs=type_specs,
                    access_key_id=account_cfg.get("access_key_id") or None,
                    access_key_secret=account_cfg.get("access_key_secret") or None,
                    user_tag=account_cfg.get("user_tag") or None,
                )
            )
    return plans


def instances_needed(node_count: int, nodes_per_host: int) -> int:
    return int(math.ceil(node_count / float(nodes_per_host)))
