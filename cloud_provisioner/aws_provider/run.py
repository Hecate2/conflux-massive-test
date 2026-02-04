from .v_switch import get_v_switchs_in_region
from .vpc import get_vpcs_in_region
from .security_group import get_security_groups_in_region
from .image import get_images_in_region


if __name__ == "__main__":
    import boto3
    
    client = boto3.client('ec2', region_name="us-west-2")
    print(client.describe_vpcs(MaxResults=1000))
    # print(get_images_in_region(client, "massive-test-seed"))
        
        
    
    # print(get_security_groups_in_region(client, "vpc-060cf3123909d0aa1"))