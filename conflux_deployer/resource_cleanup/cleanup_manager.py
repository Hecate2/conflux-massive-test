"""Resource Cleanup Manager"""

import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from loguru import logger
from conflux_deployer.configs import ConfigManager
from conflux_deployer.cloud_accounts import CloudAccountManager
from conflux_deployer.server_deployment import ServerDeployer, ServerInstance
from conflux_deployer.node_management import NodeManager
from conflux_deployer.image_management import ImageManager, ImageInfo


@dataclass
class CleanupResult:
    """Cleanup result information"""
    instances_terminated: int
    nodes_cleaned: int
    images_deleted: int
    errors: List[str]
    status: str


class ResourceCleanupManager:
    """Resource Cleanup Manager for AWS and Alibaba Cloud"""
    
    def __init__(self, config_manager: ConfigManager, cloud_account_manager: CloudAccountManager, server_deployer: ServerDeployer, node_manager: NodeManager):
        """Initialize Resource Cleanup Manager"""
        self.config_manager = config_manager
        self.cloud_account_manager = cloud_account_manager
        self.server_deployer = server_deployer
        self.node_manager = node_manager
        self.cleanup_results: Dict[str, CleanupResult] = {}
    
    def cleanup_all(self, force: bool = False) -> CleanupResult:
        """Clean up all resources"""
        logger.info(f"Starting resource cleanup (force: {force})")
        
        instances_terminated = 0
        nodes_cleaned = 0
        images_deleted = 0
        errors = []
        
        try:
            # Stop all nodes first
            try:
                self.node_manager.stop_all_nodes()
                logger.info("All nodes stopped")
            except Exception as e:
                error_msg = f"Failed to stop nodes: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
            
            # Get all instances
            instances = list(self.server_deployer.instances.values())
            logger.info(f"Found {len(instances)} instances to terminate")
            
            # Terminate instances
            if instances:
                try:
                    self.server_deployer.terminate_instances(instances)
                    instances_terminated = len(instances)
                    logger.info(f"Terminated {instances_terminated} instances")
                except Exception as e:
                    error_msg = f"Failed to terminate instances: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
            
            # Clean up node information
            instance_ids = [instance.instance_id for instance in instances]
            if instance_ids:
                try:
                    self.node_manager.cleanup_nodes(instance_ids)
                    nodes_cleaned = len(instance_ids)
                    logger.info(f"Cleaned up node information for {nodes_cleaned} instances")
                except Exception as e:
                    error_msg = f"Failed to clean up node information: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
            
            # Clear node state
            self.node_manager.clear_state()
            logger.info("Node state cleared")
            
            status = "success" if not errors else "partial_success"
        except Exception as e:
            error_msg = f"Cleanup failed with unexpected error: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            status = "failed"
        
        cleanup_result = CleanupResult(
            instances_terminated=instances_terminated,
            nodes_cleaned=nodes_cleaned,
            images_deleted=images_deleted,
            errors=errors,
            status=status
        )
        
        # Store cleanup result
        cleanup_id = f"cleanup-{int(time.time())}"
        self.cleanup_results[cleanup_id] = cleanup_result
        
        logger.info(f"Resource cleanup completed with status: {status}")
        logger.info(f"Terminated: {instances_terminated} instances, Cleaned: {nodes_cleaned} nodes, Deleted: {images_deleted} images")
        
        if errors:
            logger.warning(f"Cleanup encountered {len(errors)} errors:")
            for error in errors:
                logger.warning(f"- {error}")
        
        return cleanup_result
    
    def cleanup_images(self, cloud_provider: Optional[str] = None, region: Optional[str] = None) -> CleanupResult:
        """Clean up test images"""
        logger.info(f"Starting image cleanup for {cloud_provider or 'all providers'} in {region or 'all regions'}")
        
        images_deleted = 0
        errors = []
        
        try:
            # TODO: Implement image cleanup logic
            # For now, just simulate the process
            logger.info("Simulating image cleanup")
            time.sleep(10)  # Simulate cleanup time
            
            status = "success" if not errors else "partial_success"
        except Exception as e:
            error_msg = f"Image cleanup failed: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            status = "failed"
        
        cleanup_result = CleanupResult(
            instances_terminated=0,
            nodes_cleaned=0,
            images_deleted=images_deleted,
            errors=errors,
            status=status
        )
        
        # Store cleanup result
        cleanup_id = f"image-cleanup-{int(time.time())}"
        self.cleanup_results[cleanup_id] = cleanup_result
        
        logger.info(f"Image cleanup completed with status: {status}")
        logger.info(f"Deleted: {images_deleted} images")
        
        if errors:
            logger.warning(f"Image cleanup encountered {len(errors)} errors:")
            for error in errors:
                logger.warning(f"- {error}")
        
        return cleanup_result
    
    def get_cleanup_result(self, cleanup_id: str) -> Optional[CleanupResult]:
        """Get cleanup result by ID"""
        return self.cleanup_results.get(cleanup_id)
    
    def list_cleanup_results(self) -> List[CleanupResult]:
        """List cleanup results"""
        return list(self.cleanup_results.values())
    
    def schedule_cleanup(self, delay_seconds: int = 3600):
        """Schedule cleanup after delay"""
        logger.info(f"Scheduling cleanup in {delay_seconds} seconds")
        
        def cleanup_task():
            logger.info("Executing scheduled cleanup")
            self.cleanup_all(force=True)
        
        # TODO: Implement actual scheduling
        # For now, just log the scheduling
        logger.info("Cleanup scheduled (simulated)")
    
    def register_signal_handlers(self):
        """Register signal handlers for cleanup"""
        import signal
        
        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            logger.info(f"Received signal {signal_name}, starting cleanup")
            self.cleanup_all(force=True)
            import sys
            sys.exit(0)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("Signal handlers registered for SIGINT and SIGTERM")
