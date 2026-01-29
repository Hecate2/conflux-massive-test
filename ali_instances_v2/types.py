from dataclasses import dataclass
from typing import Dict, List

from alibabacloud_ecs20140526 import models as ecs_models


DEFAULT_COMMON_TAG_KEY = "conflux-massive-test"
DEFAULT_COMMON_TAG_VALUE = "true"
DEFAULT_USER_TAG_KEY = "user"
DEFAULT_USER_TAG_VALUE = "your_name"


@dataclass
class ZoneInfo:
    id: str
    v_switch_id: str
    
@dataclass
class RegionInfo:
    id: str
    zones: Dict[str, ZoneInfo]
    security_group_id: str
    vpc_id: str
    image_id: str
    key_pair_name: str
    key_path: str
    
    def get_zone(self, zone_id: str)-> ZoneInfo:
        return self.zones[zone_id]

@dataclass(frozen=True)
class InstanceType:
    name: str
    nodes: int
    
@dataclass(frozen=True)
class Instance:
    instance_id: str
    type: InstanceType
    
    
@dataclass
class InstanceConfig:
    instance_name_prefix: str = "conflux-massive-test"
    
    disk_size: int = 40
    internet_max_bandwidth_out: int = 100
    
    user_tag_key: str = DEFAULT_USER_TAG_KEY
    user_tag_value: str = DEFAULT_USER_TAG_VALUE
    
    @property
    def instance_tags(self) -> List[ecs_models.RunInstancesRequestTag]:
        return [
            ecs_models.RunInstancesRequestTag(key=DEFAULT_COMMON_TAG_KEY, value=DEFAULT_COMMON_TAG_VALUE),
            ecs_models.RunInstancesRequestTag(key=self.user_tag_key, value=self.user_tag_value)
        ]