
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
import tomllib

from dotenv import load_dotenv
from loguru import logger

from ..aliyun_provider.client_factory import AliyunClient
from ..aws_provider.client_factory import AwsClient
from ..host_spec import save_hosts

from .provider_interface import IEcsClient
from .instance_config import InstanceConfig
from .instance_provisioner import create_instances_in_region
from .network_infra import InfraRequest, InfraProvider
from .types import InstanceType
from .provision_config import CloudConfig, ProvisionConfig


def ensure_provider_infra(client: IEcsClient, cloud_config: CloudConfig, allow_create: bool):
    request = InfraRequest.from_config(cloud_config, allow_create=allow_create)
    provider = request.ensure_infras(client)
    logger.success(f"{cloud_config.provider} infra check pass")
    return provider


def create_instances_with_infra_provider(client: IEcsClient, cloud_config: CloudConfig, provider: InfraProvider):
    instance_config = InstanceConfig(user_tag_value=cloud_config.user_tag)
    instance_types = [InstanceType(i.name, i.nodes)
                      for i in cloud_config.instance_types]

    def _create_in_region(region_id: str, nodes: int):
        return create_instances_in_region(client, instance_config, region_info=provider.get_region(region_id), instance_types=instance_types, nodes=nodes, ssh_user=cloud_config.default_user_name, provider=cloud_config.provider)

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda reg: _create_in_region(reg.name, reg.count), cloud_config.regions))
        hosts = list(chain.from_iterable(results))
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

    with open("request_config.toml", "rb") as f:
        data = tomllib.load(f)
        config = ProvisionConfig(**data)

    providers = [
        ("aliyun", AliyunClient.load_from_env(), config.aliyun),
        ("aws", AwsClient.new(), config.aws),
    ]

    # Phase 1: ensure infra checks concurrently for all providers
    future_to_info = {}
    provider_map = {}
    with ThreadPoolExecutor(max_workers=len(providers)) as executor:
        future_to_info = {executor.submit(ensure_provider_infra, client, conf, args.allow_create): (name, client, conf) for name, client, conf in providers}
        for fut in as_completed(future_to_info):
            name, client, conf = future_to_info[fut]
            provider = fut.result()
            provider_map[name] = (client, conf, provider)

    if args.network_only:
        logger.success("Network infra ready for all providers")
        exit(0)

    # Phase 2: create instances in parallel across providers
    all_hosts = []
    with ThreadPoolExecutor(max_workers=len(provider_map)) as executor:
        future_to_name = {executor.submit(create_instances_with_infra_provider, client, conf, provider): name for name, (client, conf, provider) in provider_map.items()}
        for fut in as_completed(future_to_name):
            hosts = fut.result()
            all_hosts.extend(hosts)

    save_hosts(all_hosts, args.output_json)
