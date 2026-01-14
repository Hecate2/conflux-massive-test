"""Configuration Manager"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path

from loguru import logger


class ConfigManager:
    """Configuration Manager for Conflux Deployer"""
    
    def __init__(self, config_path: str):
        """Initialize Config Manager"""
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file"""
        if not self.config_path.exists():
            logger.error(f"Configuration file not found: {self.config_path}")
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse configuration file: {e}")
            raise
    
    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Get configuration value by key"""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_cloud_account(self, cloud_provider: str, account_name: str) -> Dict[str, Any]:
        """Get cloud account configuration"""
        accounts = self.get(f"cloud_accounts.{cloud_provider}", {})
        if account_name not in accounts:
            logger.error(f"Account {account_name} not found for {cloud_provider}")
            raise ValueError(f"Account {account_name} not found for {cloud_provider}")
        return accounts[account_name]
    
    def get_instance_config(self, instance_type: str) -> Dict[str, Any]:
        """Get instance configuration"""
        instance_configs = self.get("instance_configs", {})
        if instance_type not in instance_configs:
            logger.error(f"Instance type {instance_type} not found in configuration")
            raise ValueError(f"Instance type {instance_type} not found in configuration")
        return instance_configs[instance_type]
    
    def get_conflux_config(self) -> Dict[str, Any]:
        """Get Conflux configuration"""
        return self.get("conflux", {})
    
    def get_test_config(self, test_type: str) -> Dict[str, Any]:
        """Get test configuration"""
        test_configs = self.get("tests", {})
        if test_type not in test_configs:
            logger.error(f"Test type {test_type} not found in configuration")
            raise ValueError(f"Test type {test_type} not found in configuration")
        return test_configs[test_type]
    
    def get_region_config(self, cloud_provider: str, region: str) -> Dict[str, Any]:
        """Get region configuration"""
        regions = self.get(f"regions.{cloud_provider}", {})
        if region not in regions:
            logger.error(f"Region {region} not found for {cloud_provider}")
            raise ValueError(f"Region {region} not found for {cloud_provider}")
        return regions[region]
