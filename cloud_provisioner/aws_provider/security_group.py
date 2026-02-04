# pyright: reportTypedDictNotRequiredAccess=false

from typing import List
from ..create_instances.types import SecurityGroupInfo

from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import SecurityGroupTypeDef


def as_security_group_info(rep: SecurityGroupTypeDef):
    security_group_id = rep['GroupId']
    security_group_name = rep['GroupName']

    assert type(security_group_id) is str
    assert type(security_group_name) is str

    return SecurityGroupInfo(security_group_id=security_group_id, security_group_name=security_group_name)

def get_security_groups_in_region(client: EC2Client, vpc_id: str) -> List[SecurityGroupInfo]:
    result = []

    next_token = None
    while True:
        kwargs = dict()
        if next_token:
            kwargs['NextToken'] = next_token

        rep = client.describe_security_groups(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}], **kwargs)
        result.extend([as_security_group_info(sg) for sg in rep['SecurityGroups']])

        next_token = rep.get('NextToken')
        if not next_token:
            break

    return result


def create_security_group(client: EC2Client, vpc_id: str, security_group_name: str):
    rep = client.create_security_group(
        GroupName=security_group_name,
        Description="conflux",
        VpcId=vpc_id
    )

    security_group_id = rep['GroupId']
    assert type(security_group_id) is str

    client.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            },
            {
                'IpProtocol': 'tcp',
                'FromPort': 1024,
                'ToPort': 49151,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }
        ]
    )

    return security_group_id

    # TODO: 检查 security group
