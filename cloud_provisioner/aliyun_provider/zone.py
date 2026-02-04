from typing import List

from alibabacloud_ecs20140526.models import DescribeZonesRequest
from alibabacloud_ecs20140526.client import Client

def get_zone_ids_in_region(client: Client, region_id: str) -> List[str]:
    rep = client.describe_zones(DescribeZonesRequest(
        region_id=region_id, verbose=False))
    return [zone.zone_id for zone in rep.body.zones.zone] # pyright: ignore[reportReturnType]