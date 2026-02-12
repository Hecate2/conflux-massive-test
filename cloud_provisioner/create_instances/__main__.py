
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
import sys
import threading
import tomllib
import traceback
from typing import Optional
import os

from dotenv import load_dotenv
from loguru import logger

from ..aliyun_provider.client_factory import AliyunClient
from ..aws_provider.client_factory import AwsClient
from ..tencent_provider.client_factory import TencentClient
from ..host_spec import save_hosts
from ..provider_interface import IEcsClient

from .instance_config import InstanceConfig
from .instance_provisioner import create_instances_in_region
from .network_infra import InfraProvider, InfraRequest
from .types import InstanceType
from .provision_config import CloudConfig, ProvisionConfig, ProvisionRegionConfig


def create_instances(client: IEcsClient, cloud_config: CloudConfig, barrier: threading.Barrier, allow_create: bool, infra_only: bool):
    infra_provider = _ensure_network_infra(client, cloud_config, allow_create)

    if infra_only or infra_provider is None:
        return []
    else:
        return create_instances_in_multi_region(client, cloud_config, infra_provider)
    
def _ensure_network_infra(client: IEcsClient, cloud_config: CloudConfig, allow_create) -> Optional[InfraProvider]:
    try:
        request = InfraRequest.from_config(cloud_config, allow_create=allow_create)
        infra_provider = request.ensure_infras(client)
        logger.success(f"{cloud_config.provider} infra check pass")
        barrier.wait()
    except threading.BrokenBarrierError:
        logger.debug(f"{cloud_config.provider} quit due to other cloud providers fails")
        barrier.abort()
        return None
    except Exception as e:
        logger.error(f"Fail to build network infra: {e}")
        barrier.abort()
        print(traceback.format_exc())
        return None
    
    return infra_provider
    
def create_instances_in_multi_region(client: IEcsClient, cloud_config: CloudConfig, infra_provider: InfraProvider):
    instance_config = InstanceConfig(user_tag_value=cloud_config.user_tag)
    instance_types = [InstanceType(i.name, i.nodes)
                      for i in cloud_config.instance_types]
    regions = [reg for reg in cloud_config.regions if reg.count > 0]

    def _count_nodes(hosts):
        return sum(h.nodes_per_host for h in hosts)

    def _create_in_region(provision_config: ProvisionRegionConfig):
        region_id = provision_config.name
        return create_instances_in_region(
            client,
            instance_config,
            provision_config,
            region_info=infra_provider.get_region(region_id),
            instance_types=instance_types,
            ssh_user=cloud_config.default_user_name,
            provider=cloud_config.provider,
        )

    target_total_nodes = cloud_config.total_nodes

    region_results = []
    max_workers = min(20, max(1, len(regions)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_create_in_region, reg): reg for reg in regions}
        for future in as_completed(futures):
            reg = futures[future]
            try:
                hosts = future.result()
                region_results.append({
                    "region": reg.name,
                    "requested_nodes": reg.count,
                    "hosts": hosts,
                    "actual_nodes": _count_nodes(hosts),
                    "error": None,
                    "provision_config": reg,
                })
            except Exception as exc:
                logger.error(f"Region {reg.name} create_instances failed: {exc}")
                logger.error(traceback.format_exc())
                region_results.append({
                    "region": reg.name,
                    "requested_nodes": reg.count,
                    "hosts": [],
                    "actual_nodes": 0,
                    "error": exc,
                    "provision_config": reg,
                })

    hosts = list(chain.from_iterable(r["hosts"] for r in region_results))
    total_nodes = _count_nodes(hosts)
    shortfall = target_total_nodes - total_nodes

    underfilled = [
        r for r in region_results
        if r["actual_nodes"] < r["requested_nodes"]
    ]
    if underfilled:
        details = ", ".join(
            f"{r['region']}({r['actual_nodes']}/{r['requested_nodes']})" + ("[error]" if r["error"] is not None else "")
            for r in underfilled
        )
        logger.warning(f"Regions under target: {details}")

    if shortfall <= 0:
        logger.success(f"Multi-region launch complete: target_nodes={target_total_nodes}, actual_nodes={total_nodes}")
        return hosts

    healthy_regions = [
        r for r in region_results
        if r["error"] is None and r["actual_nodes"] >= r["requested_nodes"]
    ]
    if not healthy_regions:
        logger.error(
            f"Cannot meet total nodes: target_nodes={target_total_nodes}, actual_nodes={total_nodes}, shortfall={shortfall}. "
            f"No healthy regions available for backfill."
        )
        return hosts

    logger.warning(
        f"Total nodes shortfall={shortfall}; backfilling in healthy regions: "
        f"{[r['region'] for r in healthy_regions]}"
    )

    # Backfill loop: distribute remaining nodes across healthy regions.
    # Stop if an iteration makes no progress.
    max_backfill_workers = min(20, len(healthy_regions))
    while shortfall > 0 and healthy_regions:
        n = len(healthy_regions)
        base = shortfall // n
        rem = shortfall % n

        with ThreadPoolExecutor(max_workers=max_backfill_workers) as executor:
            backfill_futures = {}
            for i, r in enumerate(healthy_regions):
                req_nodes = base + (1 if i < rem else 0)
                if req_nodes <= 0:
                    continue
                reg_cfg: ProvisionRegionConfig = r["provision_config"].model_copy(update={"count": int(req_nodes)})
                backfill_futures[executor.submit(_create_in_region, reg_cfg)] = (r, req_nodes)

            if not backfill_futures:
                break

            progressed_nodes = 0
            newly_unhealthy = set()
            for future in as_completed(backfill_futures):
                r, req_nodes = backfill_futures[future]
                region_id = r["region"]
                try:
                    extra_hosts = future.result()
                    added_nodes = _count_nodes(extra_hosts)
                    if added_nodes <= 0:
                        logger.warning(f"Backfill made no progress in region {region_id} (requested_nodes={req_nodes})")
                        newly_unhealthy.add(region_id)
                        continue

                    hosts.extend(extra_hosts)
                    progressed_nodes += added_nodes
                    logger.success(
                        f"Backfill success in region {region_id}: requested_nodes={req_nodes}, added_nodes={added_nodes}"
                    )
                except Exception as exc:
                    logger.error(f"Backfill failed in region {region_id}: {exc}")
                    logger.error(traceback.format_exc())
                    newly_unhealthy.add(region_id)

        if progressed_nodes <= 0:
            logger.error(
                f"Backfill stalled: still shortfall={shortfall}, no further progress possible with current healthy regions."
            )
            break

        shortfall = max(0, shortfall - progressed_nodes)
        if newly_unhealthy:
            healthy_regions = [r for r in healthy_regions if r["region"] not in newly_unhealthy]

    final_nodes = _count_nodes(hosts)
    if final_nodes >= target_total_nodes:
        logger.success(f"Multi-region launch complete after backfill: target_nodes={target_total_nodes}, actual_nodes={final_nodes}")
    else:
        logger.error(f"Multi-region launch incomplete: target_nodes={target_total_nodes}, actual_nodes={final_nodes}")
    return hosts


def make_parser():
    parser = argparse.ArgumentParser(description="运行区块链节点模拟")
    parser.add_argument(
        "-c", "--request-config",
        type=str,
        default=f"./request_config.toml",
        help="节点需求配置文件路径"
    )
    parser.add_argument(
        "-o", "--output-json",
        type=str,
        default=f"./hosts.json",
        help="输出的 hosts 文件路径"
    )
    parser.add_argument(
        "--allow-create",
        action="store_true",
        help="在 Network Infra 不存在时允许创建"
    )
    parser.add_argument(
        "--network-only",
        action="store_true",
        help="只进行 Network Infra 阶段，不创建实例"
    )
    return parser


if __name__ == "__main__":    
    parser = make_parser()
    args = parser.parse_args()

    load_dotenv()
    
    from utils.logger import configure_logger
    configure_logger()

    with open("request_config.toml", "rb") as f:
        data = tomllib.load(f)
        config = ProvisionConfig(**data)
        
    user_tag_prefix = os.getenv("USER_TAG_PREFIX", "")

    cloud_tasks = []
    
    if config.aws.total_nodes > 0:
        aws_client = AwsClient.new()
        cloud_tasks.append((aws_client, config.aws))
        if not config.aws.user_tag.startswith(user_tag_prefix):
            logger.error(f"AWS user tag {config.aws.user_tag} in config file does not match the prefix in environment variable USER_TAG_PREFIX='{user_tag_prefix}'")
            sys.exit(1)
     
    if config.aliyun.total_nodes > 0:
        ali_client = AliyunClient.load_from_env()
        cloud_tasks.append((ali_client, config.aliyun))
        if not config.aliyun.user_tag.startswith(user_tag_prefix):
            logger.error(f"Aliyun User tag {config.aliyun.user_tag} in config file does not match the prefix in environment variable USER_TAG_PREFIX='{user_tag_prefix}'")
            sys.exit(1)
    
    if not args.network_only:
        total_nodes = config.aws.total_nodes + config.aliyun.total_nodes + config.tencent.total_nodes
        logger.success(f"计划启动 {total_nodes} 个节点，aws {config.aws.total_nodes}, aliyun {config.aliyun.total_nodes}, tencent {config.tencent.total_nodes}")
        
    if config.tencent.total_nodes > 0:
        tencent_client = TencentClient.load_from_env()
        cloud_tasks.append((tencent_client, config.tencent))
        
    barrier = threading.Barrier(len(cloud_tasks))
        
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(create_instances, client, cloud_config, barrier, args.allow_create, args.network_only)
            for client, cloud_config in cloud_tasks
        ]
        
        hosts = list(chain.from_iterable(future.result() for future in futures))
        
    if not args.network_only:
        save_hosts(hosts, args.output_json)
        logger.success(f"节点启动完成，节点信息已写入 {args.output_json}")
