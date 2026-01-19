from dataclasses import dataclass
from typing import Optional

DEFAULT_CONFLUX_GIT_REF = "v3.0.2"
DEFAULT_KEYPAIR_NAME = "chenxinghao-conflux-image-builder"
DEFAULT_SSH_PRIVATE_KEY = "./keys/chenxinghao-conflux-image-builder.pem"
DEFAULT_REGION_ID = "ap-southeast-3"
DEFAULT_VPC_NAME = "conflux-image-builder"
DEFAULT_VSWITCH_NAME = "conflux-image-builder"
DEFAULT_SECURITY_GROUP_NAME = "conflux-image-builder"
DEFAULT_VPC_CIDR = "10.0.0.0/16"
DEFAULT_VSWITCH_CIDR = "10.0.0.0/24"
DEFAULT_ENDPOINT = "cloudcontrol.aliyuncs.com"


@dataclass
class ImageBuildConfig:
    credentials: object
    base_image_id: Optional[str]
    instance_type: Optional[str]
    v_switch_id: Optional[str]
    security_group_id: Optional[str]
    conflux_git_ref: str = DEFAULT_CONFLUX_GIT_REF
    min_cpu_cores: int = 2
    min_memory_gb: float = 2.0
    max_memory_gb: float = 4.0
    cpu_vendor: Optional[str] = None
    key_pair_name: str = DEFAULT_KEYPAIR_NAME
    region_id: str = DEFAULT_REGION_ID
    zone_id: Optional[str] = None
    endpoint: Optional[str] = DEFAULT_ENDPOINT
    image_prefix: str = "conflux-massive-test"
    instance_name_prefix: str = "conflux-image-builder"
    internet_max_bandwidth_out: int = 10
    ssh_username: str = "root"
    ssh_private_key_path: str = DEFAULT_SSH_PRIVATE_KEY
    poll_interval: int = 5
    wait_timeout: int = 1800
    cleanup_builder_instance: bool = True
    search_all_regions: bool = False
    use_spot: bool = True
    spot_strategy: str = "SpotAsPriceGo"
    vpc_name: str = DEFAULT_VPC_NAME
    vswitch_name: str = DEFAULT_VSWITCH_NAME
    security_group_name: str = DEFAULT_SECURITY_GROUP_NAME
    vpc_cidr: str = DEFAULT_VPC_CIDR
    vswitch_cidr: str = DEFAULT_VSWITCH_CIDR
