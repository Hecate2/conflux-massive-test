"""
Cloud Provider Factory

Creates and manages cloud provider instances.
"""

from typing import Dict, Optional

from ..configs import CloudProvider, CloudCredentials, DeploymentConfig
from .base import CloudProviderBase
from .aws_provider import AWSProvider
from .alibaba_provider import AlibabaProvider


class CloudProviderFactory:
    """
    Factory for creating cloud provider instances.
    
    Maintains a cache of provider instances to avoid repeated initialization.
    """
    
    def __init__(self):
        self._providers: Dict[str, CloudProviderBase] = {}
    
    def _get_cache_key(self, provider: CloudProvider, region_id: str) -> str:
        """Generate cache key for a provider instance"""
        return f"{provider.value}:{region_id}"
    
    def get_provider(
        self,
        provider: CloudProvider,
        credentials: CloudCredentials,
        region_id: str,
    ) -> CloudProviderBase:
        """
        Get or create a cloud provider instance.
        
        Args:
            provider: Cloud provider type
            credentials: Cloud credentials
            region_id: Region ID
            
        Returns:
            CloudProviderBase instance
        """
        cache_key = self._get_cache_key(provider, region_id)
        
        if cache_key not in self._providers:
            if provider == CloudProvider.AWS:
                self._providers[cache_key] = AWSProvider(credentials, region_id)
            elif provider == CloudProvider.ALIBABA:
                self._providers[cache_key] = AlibabaProvider(credentials, region_id)
            else:
                raise ValueError(f"Unsupported cloud provider: {provider}")
            
            # Initialize the client
            self._providers[cache_key].initialize_client()
        
        return self._providers[cache_key]
    
    def get_provider_from_config(
        self,
        config: DeploymentConfig,
        provider: CloudProvider,
        region_id: str,
    ) -> CloudProviderBase:
        """
        Get or create a cloud provider instance from deployment config.
        
        Args:
            config: Deployment configuration
            provider: Cloud provider type
            region_id: Region ID
            
        Returns:
            CloudProviderBase instance
        """
        if provider not in config.credentials:
            raise ValueError(f"No credentials found for provider: {provider}")
        
        return self.get_provider(provider, config.credentials[provider], region_id)
    
    def clear_cache(self) -> None:
        """Clear all cached provider instances"""
        self._providers.clear()


# Global factory instance
_factory: Optional[CloudProviderFactory] = None


def get_cloud_factory() -> CloudProviderFactory:
    """Get the global cloud provider factory instance"""
    global _factory
    if _factory is None:
        _factory = CloudProviderFactory()
    return _factory


def get_provider(
    provider: CloudProvider,
    credentials: CloudCredentials,
    region_id: str,
) -> CloudProviderBase:
    """
    Convenience function to get a cloud provider instance.
    
    Args:
        provider: Cloud provider type
        credentials: Cloud credentials
        region_id: Region ID
        
    Returns:
        CloudProviderBase instance
    """
    return get_cloud_factory().get_provider(provider, credentials, region_id)
