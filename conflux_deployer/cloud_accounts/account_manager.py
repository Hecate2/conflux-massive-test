"""Cloud Account Manager"""

from typing import Dict, Any, Optional
import boto3
from alibabacloud_ecs20140526 import client as ecs_client
from alibabacloud_tea_openapi import models as open_api_models

from loguru import logger
from conflux_deployer.configs.config_manager import ConfigManager


class CloudAccountManager:
    """Cloud Account Manager for AWS and Alibaba Cloud"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize Cloud Account Manager"""
        self.config_manager = config_manager
        self.clients: Dict[str, Any] = {}
    
    def get_aws_client(self, service: str, region: str, account_name: str = "default") -> Any:
        """Get AWS client for specific service"""
        client_key = f"aws_{service}_{region}_{account_name}"
        if client_key in self.clients:
            return self.clients[client_key]
        
        # Get account configuration
        account_config = self.config_manager.get_cloud_account("aws", account_name)
        
        # Create AWS client
        session = boto3.Session(
            aws_access_key_id=account_config.get("access_key"),
            aws_secret_access_key=account_config.get("secret_key"),
            region_name=region
        )
        
        client = session.client(service)
        self.clients[client_key] = client
        logger.info(f"AWS {service} client created for region {region} using account {account_name}")
        return client
    
    def get_alibaba_client(self, region: str, account_name: str = "default") -> Any:
        """Get Alibaba Cloud client"""
        client_key = f"alibaba_{region}_{account_name}"
        if client_key in self.clients:
            return self.clients[client_key]
        
        # Get account configuration
        account_config = self.config_manager.get_cloud_account("alibaba", account_name)
        
        # Validate and extract keys
        try:
            access_key = account_config["access_key"]
            secret_key = account_config["secret_key"]
        except KeyError as e:
            raise ValueError(f"Missing required key {e.args[0]} for alibaba account {account_name}") from e

        # Create Alibaba Cloud client
        config = open_api_models.Config(
            access_key_id=str(access_key),
            access_key_secret=str(secret_key),
            region_id=region
        )

        client = ecs_client.Client(config)
        self.clients[client_key] = client
        logger.info(f"Alibaba Cloud client created for region {region} using account {account_name}")
        return client
    
    def switch_account(self, cloud_provider: str, account_name: str) -> None:
        """Switch to specified cloud account"""
        # Clear existing clients for this provider
        provider_prefix = f"{cloud_provider}_"
        self.clients = {k: v for k, v in self.clients.items() if not k.startswith(provider_prefix)}
        
        logger.info(f"Switched to {cloud_provider} account: {account_name}")
    
    def validate_account(self, cloud_provider: str, account_name: str) -> bool:
        """Validate cloud account credentials"""
        try:
            if cloud_provider == "aws":
                # Test AWS account by getting EC2 client
                client = self.get_aws_client("ec2", "us-west-2", account_name)
                client.describe_regions()
                logger.info(f"AWS account {account_name} validated successfully")
                return True
            elif cloud_provider == "alibaba":
                # Test Alibaba account by getting ECS client
                client = self.get_alibaba_client("us-west-1", account_name)
                # Try to describe regions
                client.describe_regions()
                logger.info(f"Alibaba Cloud account {account_name} validated successfully")
                return True
            else:
                logger.error(f"Unsupported cloud provider: {cloud_provider}")
                return False
        except Exception as e:
            logger.error(f"Failed to validate {cloud_provider} account {account_name}: {e}")
            return False
