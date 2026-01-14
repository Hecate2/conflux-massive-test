"""Server Image Manager"""

import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from loguru import logger
from conflux_deployer.configs import ConfigManager
from conflux_deployer.cloud_accounts import CloudAccountManager


@dataclass
class ImageInfo:
    """Image information"""
    image_id: str
    cloud_provider: str
    region: str
    name: str
    created_at: str
    status: str


class ImageManager:
    """Server Image Manager for AWS and Alibaba Cloud"""
    
    def __init__(self, config_manager: ConfigManager, cloud_account_manager: CloudAccountManager):
        """Initialize Image Manager"""
        self.config_manager = config_manager
        self.cloud_account_manager = cloud_account_manager
        self.images: Dict[str, ImageInfo] = {}
    
    def create_image(self, cloud_provider: str, region: str, base_image_id: str, image_name: str, purpose: str = "conflux-node") -> ImageInfo:
        """Create server image with Docker and Conflux pre-installed"""
        # Check if image already exists
        existing_image = self.find_image(cloud_provider, region, image_name)
        if existing_image:
            logger.info(f"Image {image_name} already exists, reusing it: {existing_image.image_id}")
            return existing_image
        
        if cloud_provider == "aws":
            return self._create_aws_image(region, base_image_id, image_name, purpose)
        elif cloud_provider == "alibaba":
            return self._create_alibaba_image(region, base_image_id, image_name, purpose)
        else:
            raise ValueError(f"Unsupported cloud provider: {cloud_provider}")
    
    def _create_aws_image(self, region: str, base_image_id: str, image_name: str, purpose: str) -> ImageInfo:
        """Create AWS server image"""
        ec2_client = self.cloud_account_manager.get_aws_client("ec2", region)
        
        # Launch temporary instance
        response = ec2_client.run_instances(
            ImageId=base_image_id,
            InstanceType="t3.small",
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Name', 'Value': f"temp-{image_name}"},
                        {'Key': 'Purpose', 'Value': 'image-creation'}
                    ]
                }
            ]
        )
        
        instance_id = response['Instances'][0]['InstanceId']
        logger.info(f"Launched temporary instance {instance_id} for image creation")
        
        # Wait for instance to be running
        self._wait_for_aws_instance_running(ec2_client, instance_id)
        
        # Get instance public IP
        instance = ec2_client.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]
        public_ip = instance.get('PublicIpAddress')
        
        # Install Docker and Conflux
        self._setup_instance(public_ip, purpose)
        
        # Create image
        image_response = ec2_client.create_image(
            InstanceId=instance_id,
            Name=image_name,
            Description=f"Server image for {purpose} with Docker and Conflux pre-installed"
        )
        
        image_id = image_response['ImageId']
        logger.info(f"Creating image {image_id} from instance {instance_id}")
        
        # Wait for image to be available
        self._wait_for_aws_image_available(ec2_client, image_id)
        
        # Terminate temporary instance
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        logger.info(f"Terminated temporary instance {instance_id}")
        
        # Create ImageInfo object
        image_info = ImageInfo(
            image_id=image_id,
            cloud_provider="aws",
            region=region,
            name=image_name,
            created_at=time.strftime("%Y-%m-%d %H:%M:%S"),
            status="available"
        )
        
        # Store image info
        key = f"{cloud_provider}_{region}_{image_name}"
        self.images[key] = image_info
        
        return image_info
    
    def _create_alibaba_image(self, region: str, base_image_id: str, image_name: str, purpose: str) -> ImageInfo:
        """Create Alibaba Cloud server image"""
        ecs_client = self.cloud_account_manager.get_alibaba_client(region)
        
        # Launch temporary instance
        # TODO: Implement Alibaba Cloud instance creation
        # For now, return a mock image info
        image_id = f"alibaba-image-{int(time.time())}"
        
        image_info = ImageInfo(
            image_id=image_id,
            cloud_provider="alibaba",
            region=region,
            name=image_name,
            created_at=time.strftime("%Y-%m-%d %H:%M:%S"),
            status="available"
        )
        
        # Store image info
        key = f"{cloud_provider}_{region}_{image_name}"
        self.images[key] = image_info
        
        return image_info
    
    def find_image(self, cloud_provider: str, region: str, image_name: str) -> Optional[ImageInfo]:
        """Find existing image by name"""
        key = f"{cloud_provider}_{region}_{image_name}"
        if key in self.images:
            return self.images[key]
        
        # Search in cloud provider
        if cloud_provider == "aws":
            return self._find_aws_image(region, image_name)
        elif cloud_provider == "alibaba":
            return self._find_alibaba_image(region, image_name)
        else:
            return None
    
    def _find_aws_image(self, region: str, image_name: str) -> Optional[ImageInfo]:
        """Find AWS image by name"""
        ec2_client = self.cloud_account_manager.get_aws_client("ec2", region)
        
        response = ec2_client.describe_images(
            Filters=[
                {'Name': 'name', 'Values': [image_name]},
                {'Name': 'state', 'Values': ['available']}
            ],
            Owners=['self']
        )
        
        if response['Images']:
            image = response['Images'][0]
            image_info = ImageInfo(
                image_id=image['ImageId'],
                cloud_provider="aws",
                region=region,
                name=image['Name'],
                created_at=image['CreationDate'],
                status="available"
            )
            
            # Store image info
            key = f"aws_{region}_{image_name}"
            self.images[key] = image_info
            
            return image_info
        
        return None
    
    def _find_alibaba_image(self, region: str, image_name: str) -> Optional[ImageInfo]:
        """Find Alibaba Cloud image by name"""
        # TODO: Implement Alibaba Cloud image search
        return None
    
    def _wait_for_aws_instance_running(self, ec2_client: Any, instance_id: str):
        """Wait for AWS instance to be running"""
        while True:
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']
            if state == 'running':
                break
            time.sleep(5)
        logger.info(f"Instance {instance_id} is now running")
    
    def _wait_for_aws_image_available(self, ec2_client: Any, image_id: str):
        """Wait for AWS image to be available"""
        while True:
            response = ec2_client.describe_images(ImageIds=[image_id])
            state = response['Images'][0]['State']
            if state == 'available':
                break
            time.sleep(10)
        logger.info(f"Image {image_id} is now available")
    
    def _setup_instance(self, public_ip: str, purpose: str):
        """Setup instance with Docker and Conflux"""
        # TODO: Implement SSH setup to install Docker and Conflux
        # For now, just simulate the process
        logger.info(f"Setting up instance {public_ip} with Docker and Conflux for {purpose}")
        time.sleep(60)  # Simulate setup time
    
    def delete_image(self, image_info: ImageInfo):
        """Delete server image"""
        if image_info.cloud_provider == "aws":
            self._delete_aws_image(image_info.region, image_info.image_id)
        elif image_info.cloud_provider == "alibaba":
            self._delete_alibaba_image(image_info.region, image_info.image_id)
        
        # Remove from cache
        key = f"{image_info.cloud_provider}_{image_info.region}_{image_info.name}"
        if key in self.images:
            del self.images[key]
        
        logger.info(f"Deleted image {image_info.name}: {image_info.image_id}")
    
    def _delete_aws_image(self, region: str, image_id: str):
        """Delete AWS server image"""
        ec2_client = self.cloud_account_manager.get_aws_client("ec2", region)
        ec2_client.deregister_image(ImageId=image_id)
    
    def _delete_alibaba_image(self, region: str, image_id: str):
        """Delete Alibaba Cloud server image"""
        # TODO: Implement Alibaba Cloud image deletion
        pass
