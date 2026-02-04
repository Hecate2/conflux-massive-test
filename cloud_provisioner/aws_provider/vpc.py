# pyright: reportTypedDictNotRequiredAccess=false
from typing import List

from ..create_instances.types import VpcInfo
from utils.wait_until import wait_until

from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import VpcTypeDef


def as_vpc_info(vpc_dict: VpcTypeDef):
    vpc_id = vpc_dict['VpcId'] 
    
    vpc_name = ''
    if 'Tags' in vpc_dict:
        for tag in vpc_dict['Tags']:
            if tag['Key'] == 'Name':
                vpc_name = tag['Value']
                break
    
    state = vpc_dict['State']
    
    assert type(vpc_id) is str
    assert type(vpc_name) is str
    assert type(state) is str
    
    return VpcInfo(vpc_id=vpc_id, vpc_name=vpc_name)

def get_vpcs_in_region(client: EC2Client) -> List[VpcInfo]:
    result = []
    
    next_token = None
    while True:
        if next_token:
            response = client.describe_vpcs(MaxResults=1000, NextToken=next_token)
        else:
            response = client.describe_vpcs(MaxResults=1000)
        
        result.extend([as_vpc_info(vpc) for vpc in response['Vpcs']])
        
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return result

def create_vpc(client: EC2Client, vpc_name: str, cidr_block: str):
    response = client.create_vpc(CidrBlock=cidr_block)
    vpc_id = response['Vpc']['VpcId']
    
    assert type(vpc_id) is str
    
    client.create_tags(Resources=[vpc_id], Tags=[{'Key': 'Name', 'Value': vpc_name}])
    
    def _available() -> bool:
        resp = client.describe_vpcs(VpcIds=[vpc_id])
        vpcs = resp['Vpcs']
        return len(vpcs) > 0 and vpcs[0]['State'] == 'available'
    
    wait_until(_available, timeout=120, retry_interval=3)
    
    # 配置互联网访问
    igw_response = client.create_internet_gateway()
    igw_id = igw_response['InternetGateway']['InternetGatewayId']
    client.create_tags(Resources=[igw_id], Tags=[{'Key': 'Name', 'Value': f'{vpc_name}-igw'}])
    client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    
    # 配置主路由表
    route_tables = client.describe_route_tables(Filters=[
        {'Name': 'vpc-id', 'Values': [vpc_id]},
        {'Name': 'association.main', 'Values': ['true']}
    ])
    main_route_table_id = route_tables['RouteTables'][0]['RouteTableId']
    client.create_route(
        RouteTableId=main_route_table_id,
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=igw_id
    )
    
    
    return vpc_id

