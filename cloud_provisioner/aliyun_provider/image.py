# pyright: reportOptionalOperand=false

from typing import List
from alibabacloud_ecs20140526.models import DescribeImagesRequest, DescribeImagesResponseBodyImagesImage
from alibabacloud_ecs20140526.client import Client

from cloud_provisioner.create_instances.types import ImageInfo


def as_image_info(rep: DescribeImagesResponseBodyImagesImage):
    assert type(rep.image_id) is str
    assert type(rep.image_name) is str
    
    return ImageInfo(image_id=rep.image_id, image_name=rep.image_name)


def get_images_in_region(client: Client, region_id: str, image_name: str) -> List[ImageInfo]:    
    result = []
    
    page_number = 1
    while True:
        rep = client.describe_images(DescribeImagesRequest(region_id=region_id, image_name=image_name, image_owner_alias="self", page_number=page_number, page_size=50))

        result.extend([as_image_info(vpc) for vpc in rep.body.images.image])
        if rep.body.total_count <= page_number * 50: 
            break
        page_number += 1
    
    return result