# pyright: reportTypedDictNotRequiredAccess=false


from typing import List
from cloud_provisioner.create_instances.types import ImageInfo
from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import ImageTypeDef


def as_image_info(image: ImageTypeDef):
    assert type(image['ImageId']) is str
    assert type(image['Name']) is str

    return ImageInfo(image_id=image['ImageId'], image_name=image['Name'])


def get_images_in_region(client: EC2Client, image_name: str) -> List[ImageInfo]:
    result = []

    next_token = None
    while True:
        kwargs = dict()
        if next_token:
            kwargs['NextToken'] = next_token

        response = client.describe_images(Filters=[
            {'Name': 'name', 'Values': [image_name]},
        ],
            Owners=['self'],
            MaxResults=1000, **kwargs)

        result.extend([as_image_info(image) for image in response['Images']])

        next_token = response.get('NextToken')
        if not next_token:
            break

    return result
