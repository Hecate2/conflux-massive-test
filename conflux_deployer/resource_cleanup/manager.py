"""
Resource Cleanup Module

Handles cleanup of all cloud resources:
- Terminate instances
- Delete security groups
- Delete images (optional)
- Clean up state

CRITICAL: This module ensures resources are cleaned up regardless of
test success/failure to prevent ongoing cloud charges.
"""

import time
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from ..configs import (
    DeploymentConfig,
    CloudProvider,
    InstanceInfo,
    InstanceState,
    CleanupConfig,
)
from ..cloud import CloudProviderBase, get_cloud_factory
from ..configs.loader import StateManager


class ResourceCleanupManager:
    """
    Manages cleanup of all cloud resources.
    
    This class is critical for ensuring that cloud resources are properly
    cleaned up to prevent ongoing charges. It supports:
    
    - Graceful cleanup with retries
    - Force cleanup for stuck resources
    - Recovery from partial cleanup state
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        state_manager: StateManager,
    ):
        """
        Initialize the cleanup manager.
        
        Args:
            config: Deployment configuration
            state_manager: State manager for persistence
        """
        self.config = config
        self.state_manager = state_manager
        self.factory = get_cloud_factory()
        self._cleanup_config = config.cleanup
    
    def _get_provider(self, provider: CloudProvider, region_id: str) -> CloudProviderBase:
        """Get a cloud provider instance"""
        return self.factory.get_provider(
            provider,
            self.config.credentials[provider],
            region_id,
        )
    
    def _get_security_group_name(self) -> str:
        """Get the security group name for this deployment"""
        return f"conflux-{self.config.deployment_id}"
    
    def terminate_all_instances(
        self, 
        force: bool = False,
    ) -> Dict[str, bool]:
        """
        Terminate all instances associated with this deployment.
        
        Args:
            force: If True, skip confirmation and retry harder
            
        Returns:
            Dict mapping instance_id to termination success
        """
        results: Dict[str, bool] = {}
        
        state = self.state_manager.state
        if not state:
            logger.info("No state found, searching for instances by tag...")
            return self._terminate_by_tag()
        
        instances = state.instances
        if not instances:
            logger.info("No instances in state to terminate")
            return results
        
        # Group instances by provider/region
        by_region: Dict[str, List[InstanceInfo]] = {}
        for instance in instances:
            region_key = f"{instance.provider.value}:{instance.region_id}"
            if region_key not in by_region:
                by_region[region_key] = []
            by_region[region_key].append(instance)
        
        # Terminate instances in each region
        retry_count = self._cleanup_config.retry_attempts if not force else 5
        
        for region_key, region_instances in by_region.items():
            # Skip already terminated
            active_instances = [
                i for i in region_instances 
                if i.state != InstanceState.TERMINATED
            ]
            
            if not active_instances:
                continue
            
            provider = active_instances[0].provider
            region_id = active_instances[0].region_id
            instance_ids = [i.instance_id for i in active_instances]
            
            logger.info(f"Terminating {len(instance_ids)} instances in {region_key}")
            
            try:
                cloud = self._get_provider(provider, region_id)
                
                for attempt in range(retry_count):
                    try:
                        term_results = cloud.terminate_instances(instance_ids)
                        
                        for instance_id, success in term_results.items():
                            results[instance_id] = success
                            if success:
                                self.state_manager.update_instance(
                                    instance_id,
                                    state=InstanceState.TERMINATED,
                                )
                        
                        # Check if all terminated
                        if all(results.get(i, False) for i in instance_ids):
                            break
                        
                        if attempt < retry_count - 1:
                            time.sleep(5)
                            
                    except Exception as e:
                        logger.warning(f"Termination attempt {attempt + 1} failed: {e}")
                        if attempt < retry_count - 1:
                            time.sleep(5)
                        else:
                            for instance_id in instance_ids:
                                if instance_id not in results:
                                    results[instance_id] = False
                            
            except Exception as e:
                logger.error(f"Failed to terminate instances in {region_key}: {e}")
                for instance_id in instance_ids:
                    results[instance_id] = False
        
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Terminated {success_count}/{len(results)} instances")
        
        return results
    
    def _terminate_by_tag(self) -> Dict[str, bool]:
        """
        Find and terminate instances by deployment tag.
        
        This is used when state is not available but instances may still exist.
        
        Returns:
            Dict mapping instance_id to termination success
        """
        results: Dict[str, bool] = {}
        
        # Search in each configured region
        for region_config in self.config.regions:
            try:
                cloud = self._get_provider(region_config.provider, region_config.region_id)
                
                # Find instances by deployment ID
                deployment_id_str = str(self.config.deployment_id) if self.config.deployment_id is not None else ""
                instances = cloud.list_instances_by_tag(
                    "DeploymentId",
                    deployment_id_str,
                )
                
                if not instances:
                    # Also try by name prefix
                    instance_prefix = str(self.config.instance_name_prefix) if self.config.instance_name_prefix is not None else ""
                    instances = cloud.list_instances_by_tag(
                        "DeploymentPrefix",
                        instance_prefix,
                    )
                
                if instances:
                    instance_ids = [i.instance_id for i in instances]
                    logger.info(
                        f"Found {len(instance_ids)} instances in "
                        f"{region_config.region_id}"
                    )
                    
                    term_results = cloud.terminate_instances(instance_ids)
                    results.update(term_results)
                    
            except Exception as e:
                logger.warning(
                    f"Failed to search/terminate in "
                    f"{region_config.provider.value}/{region_config.region_id}: {e}"
                )
        
        return results
    
    def delete_security_groups(self) -> Dict[str, bool]:
        """
        Delete security groups created by this deployment.
        
        Returns:
            Dict mapping security_group_id to deletion success
        """
        results: Dict[str, bool] = {}
        sg_name = self._get_security_group_name()
        
        # Search in each configured region
        for region_config in self.config.regions:
            try:
                cloud = self._get_provider(region_config.provider, region_config.region_id)
                
                # Find security group
                sg_id = cloud.find_security_group_by_name(sg_name)
                
                if sg_id:
                    logger.info(f"Deleting security group {sg_id} in {region_config.region_id}")
                    
                    # Wait a bit for instances to fully terminate
                    time.sleep(10)
                    
                    try:
                        success = cloud.delete_security_group(sg_id)
                        results[sg_id] = success
                    except Exception as e:
                        logger.warning(f"Failed to delete security group {sg_id}: {e}")
                        results[sg_id] = False
                        
            except Exception as e:
                logger.warning(
                    f"Failed to search/delete security group in "
                    f"{region_config.provider.value}/{region_config.region_id}: {e}"
                )
        
        return results
    
    def delete_images(self) -> Dict[str, bool]:
        """
        Delete images created by this deployment.
        
        Returns:
            Dict mapping image_id to deletion success
        """
        results: Dict[str, bool] = {}
        
        state = self.state_manager.state
        if not state or not state.images:
            logger.info("No images in state to delete")
            return results
        
        for provider_key, regions in state.images.items():
            provider = CloudProvider(provider_key)
            
            for region_id, image_id in regions.items():
                try:
                    cloud = self._get_provider(provider, region_id)
                    
                    logger.info(f"Deleting image {image_id} in {provider_key}/{region_id}")
                    success = cloud.delete_image(image_id)
                    results[image_id] = success
                    
                except Exception as e:
                    logger.warning(f"Failed to delete image {image_id}: {e}")
                    results[image_id] = False
        
        return results
    
    def cleanup_all(
        self, 
        force: bool = False,
        delete_images: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Clean up all resources.
        
        This is the main cleanup method that should be called to ensure
        all resources are properly cleaned up.
        
        Args:
            force: If True, retry harder and skip confirmations
            delete_images: Whether to delete images (defaults to config)
            
        Returns:
            Dict with cleanup results for each resource type
        """
        logger.info("Starting resource cleanup...")
        
        self.state_manager.update_phase("cleanup")
        
        results = {
            "instances": {},
            "security_groups": {},
            "images": {},
            "errors": [],
        }
        
        # 1. Terminate instances first
        try:
            results["instances"] = self.terminate_all_instances(force=force)
        except Exception as e:
            error_msg = f"Instance termination error: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        
        # 2. Wait for instances to terminate
        logger.info("Waiting for instances to terminate...")
        time.sleep(self._cleanup_config.grace_period_seconds)
        
        # 3. Delete security groups
        try:
            results["security_groups"] = self.delete_security_groups()
        except Exception as e:
            error_msg = f"Security group deletion error: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        
        # 4. Delete images if requested
        should_delete_images = (
            delete_images if delete_images is not None 
            else self._cleanup_config.delete_images
        )
        
        if should_delete_images:
            try:
                results["images"] = self.delete_images()
            except Exception as e:
                error_msg = f"Image deletion error: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        # 5. Update state
        self.state_manager.update_phase("completed")
        
        # 6. Summary
        instances_success = sum(1 for v in results["instances"].values() if v)
        sg_success = sum(1 for v in results["security_groups"].values() if v)
        images_success = sum(1 for v in results["images"].values() if v)
        
        logger.info(
            f"Cleanup summary: "
            f"Instances: {instances_success}/{len(results['instances'])}, "
            f"Security Groups: {sg_success}/{len(results['security_groups'])}, "
            f"Images: {images_success}/{len(results['images'])}, "
            f"Errors: {len(results['errors'])}"
        )
        
        return results
    
    def emergency_cleanup(self) -> Dict[str, Any]:
        """
        Emergency cleanup - forcefully terminate everything.
        
        Use this when normal cleanup fails or when resources are stuck.
        
        Returns:
            Cleanup results
        """
        logger.warning("Running EMERGENCY CLEANUP - this will force terminate all resources")
        
        return self.cleanup_all(force=True, delete_images=False)
    
    def estimate_running_cost(self) -> Dict[str, float]:
        """
        Estimate the running cost of current resources.
        
        Returns:
            Dict with cost estimates
        """
        state = self.state_manager.state
        if not state or not state.instances:
            return {"hourly_cost_usd": 0.0, "daily_cost_usd": 0.0}
        
        # Simple estimation based on instance types
        # These are approximate on-demand prices
        AWS_PRICES = {
            "t3.medium": 0.0416,
            "m6i.2xlarge": 0.384,
            "m7i.2xlarge": 0.403,
            "m6i.4xlarge": 0.768,
            "m7i.4xlarge": 0.806,
        }
        
        ALIBABA_PRICES = {
            "ecs.t5-lc1m2.small": 0.01,
            "ecs.g7.2xlarge": 0.30,
            "ecs.g7.4xlarge": 0.60,
            "ecs.g8i.2xlarge": 0.35,
        }
        
        hourly_cost = 0.0
        
        for instance in state.instances:
            if instance.state == InstanceState.TERMINATED:
                continue
            
            if instance.provider == CloudProvider.AWS:
                hourly_cost += AWS_PRICES.get(instance.instance_type, 0.5)
            else:
                hourly_cost += ALIBABA_PRICES.get(instance.instance_type, 0.3)
        
        return {
            "hourly_cost_usd": hourly_cost,
            "daily_cost_usd": hourly_cost * 24,
        }
