from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import time
from typing import Callable, Dict, List, Any
from alibabacloud_ecs20140526.models import DescribeInstancesRequest, DeleteInstancesRequest, DescribeInstancesResponseBodyInstancesInstance, DescribeInstancesResponseBodyInstancesInstanceTagsTag
from alibabacloud_ecs20140526.client import Client as EcsClient
from dotenv import load_dotenv
from loguru import logger

from ali_instances_v2.client_factory import ClientFactory
from ali_instances_v2.types import DEFAULT_COMMON_TAG_KEY, DEFAULT_COMMON_TAG_VALUE, DEFAULT_USER_TAG_KEY

REGIONS = [
    "ap-southeast-5",  # Indonesia
    "ap-southeast-3",  # Malaysia
    "ap-southeast-6",  # Philippines
    "ap-southeast-7",  # Thailand
    "ap-northeast-2",  # Korea
    "ap-southeast-1",  # Singapore
    "me-east-1",  
]


@dataclass
class InstanceInfo:
    instance_id: str
    instance_name: str
    tags: Dict[str, str]
    
    @classmethod
    def from_api(cls, rep: DescribeInstancesResponseBodyInstancesInstance):
        if rep.tags:
            tags = {tag.tag_key: tag.tag_value for tag in rep.tags.tag}
        else:
            tags = dict()
        return InstanceInfo(instance_id=rep.instance_id, instance_name=rep.instance_name, tags=tags)
    
def _get_instances(c: EcsClient, region_id: str) -> List[InstanceInfo]:
    instances = []
    page_number = 1
    while True:
        rep = c.describe_instances(DescribeInstancesRequest(region_id=region_id, page_number=page_number, page_size=50))
        instances.extend([InstanceInfo.from_api(instance) for instance in rep.body.instances.instance])
        
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    return instances
    
def _delete_instances(c: EcsClient, region_id: str, instances: List[InstanceInfo]):
    for i in range(0, len(instances), 100):
        instance_ids = [instance.instance_id for instance in instances[i:i+100]]
        while True: 
            try: 
                c.delete_instances(DeleteInstancesRequest(region_id = region_id, force_stop=True, force=True, instance_id=instance_ids))
                break
            except Exception as e:
                code = getattr(e, "code")
                if code == "IncorrectInstanceStatus.Initializing":
                    logger.warning(f"Some instances in region {region_id} is still initializing, waiting_retry")
            time.sleep(5)
    
    
def _delete_in_region(region_id: str, factory: ClientFactory, predicate: Callable[[InstanceInfo], bool]):
    logger.info(f"Cleanup region {region_id}")
    client = factory.build(region_id)
    instances = _get_instances(client, region_id)
    instances = list(filter(predicate, instances))
    if len(instances) > 0:
        logger.debug(f"{len(instances)} instances to stop: {instances}")
        _delete_instances(client, region_id, instances)
    logger.success(f"Cleanup region {region_id} done")

def delete_instances(factory: ClientFactory, predicate: Callable[[InstanceInfo], bool]):
    with ThreadPoolExecutor(max_workers=10) as executor:
        _ = list(executor.map(lambda region: _delete_in_region(region, factory, predicate), REGIONS))
        
        
def check_tag(instance: InstanceInfo, user_prefix: str):
    return instance.tags.get(DEFAULT_COMMON_TAG_KEY) == DEFAULT_COMMON_TAG_VALUE and instance.tags.get(DEFAULT_USER_TAG_KEY, "").startswith(user_prefix)
    
        
if __name__ == "__main__":
    load_dotenv()
    factory = ClientFactory.load_from_env()
    
    delete_instances(factory, lambda instance: check_tag(instance, "lichenxing-alpha"))
        
