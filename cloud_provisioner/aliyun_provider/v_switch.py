# pyright: reportOptionalOperand=false

from typing import List
from alibabacloud_ecs20140526.models import DescribeVSwitchesResponseBodyVSwitchesVSwitch, DescribeVSwitchesRequest, CreateVSwitchRequest
from alibabacloud_ecs20140526.client import Client

from ..create_instances.types import VSwitchInfo
from utils.wait_until import wait_until



def as_vswitch_info(rep: DescribeVSwitchesResponseBodyVSwitchesVSwitch):
    assert type(rep.v_switch_id) is str
    assert type(rep.v_switch_name) is str
    assert type(rep.zone_id) is str
    assert type(rep.status) is str
    assert type(rep.cidr_block) is str
    
    return VSwitchInfo(v_switch_id=rep.v_switch_id, v_switch_name=rep.v_switch_name, zone_id=rep.zone_id, status=rep.status, cidr_block=rep.cidr_block)
    


def get_v_switchs_in_region(client: Client, region_id: str, vpc_id: str) -> List[VSwitchInfo]:
    result = []
    
    page_number = 1
    while True:
        rep = client.describe_vswitches(DescribeVSwitchesRequest(region_id=region_id, vpc_id=vpc_id, page_number=page_number, page_size=50))
        result.extend([as_vswitch_info(v_switch) for v_switch in rep.body.v_switches.v_switch])
        if rep.body.total_count <= page_number * 50:
            break
        page_number += 1
        
    return result



def create_v_switch(client: Client, region_id: str, zone_id: str, vpc_id: str, v_switch_name: str, cidr_block: str):    
    rep = client.create_vswitch(CreateVSwitchRequest(region_id=region_id, vpc_id=vpc_id, zone_id=zone_id, v_switch_name=v_switch_name, cidr_block=cidr_block))
    v_switch_id = rep.body.v_switch_id
    
    assert type(v_switch_id) is str
    
    def _available():
        resp = client.describe_vswitches(DescribeVSwitchesRequest(region_id=region_id, v_switch_id=v_switch_id))
        v_switches = resp.body.v_switches.v_switch
        return len(v_switches) > 0 and v_switches[0].status == "Available"
    
    wait_until(_available, timeout=120, retry_interval=3)
    
    return v_switch_id
