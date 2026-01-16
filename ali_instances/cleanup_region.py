import argparse
from loguru import logger
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models

from ali_instances.ecs_common import create_client, delete_instance, load_ali_credentials


def list_instances(client: EcsClient, region_id: str, name_prefix: str) -> list[str]:
    request = ecs_models.DescribeInstancesRequest(region_id=region_id, page_size=100)
    response = client.describe_instances(request)
    instances = response.body.instances.instance if response.body and response.body.instances else []
    return [
        instance.instance_id
        for instance in instances
        if instance.instance_id and (instance.instance_name or "").startswith(name_prefix)
    ]


def release_instances(client: EcsClient, region_id: str, name_prefix: str) -> None:
    instance_ids = list_instances(client, region_id, name_prefix)
    logger.info(f"found {len(instance_ids)} instances in {region_id} with prefix {name_prefix}")
    for instance_id in instance_ids:
        logger.info(f"releasing instance {instance_id}")
        delete_instance(client, region_id, instance_id)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Release ECS instances in a region by name prefix")
    parser.add_argument("--region-id", default="ap-southeast-3")
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--name-prefix", default="conflux-massive-test")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    credentials = load_ali_credentials()
    client = create_client(credentials, args.region_id, args.endpoint)
    release_instances(client, args.region_id, args.name_prefix)


if __name__ == "__main__":
    main()
