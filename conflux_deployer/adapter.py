"""
Adapter Module

Bridges the conflux_deployer framework with the existing remote_simulation tests.
This allows using the new multi-cloud deployment infrastructure while keeping
the existing test logic in remote_simulation.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
import json

from loguru import logger

from conflux_deployer import (
    ConfluxDeployer,
    DeploymentConfig,
    CloudProvider,
    CloudCredentials,
    RegionConfig,
    ImageConfig,
    ConfluxNodeConfig,
    NetworkConfig,
    TestConfig,
    CleanupConfig,
    InstanceInfo,
    InstanceState,
)
from conflux_deployer.configs.loader import ConfigLoader, StateManager


@dataclass
class LegacyInstances:
    """
    Adapter class that mimics the old aws_instances.Instances interface
    but uses the new conflux_deployer framework underneath.
    """
    deployer: ConfluxDeployer
    instances: List[InstanceInfo]
    
    @property
    def ip_addresses(self) -> List[str]:
        """Get IP addresses of all instances (compatible with old interface)"""
        return [i.public_ip for i in self.instances if i.public_ip]
    
    @property
    def instance_ids(self) -> List[str]:
        """Get instance IDs"""
        return [i.instance_id for i in self.instances]
    
    @property
    def config(self) -> DeploymentConfig:
        """Get deployment config"""
        return self.deployer.config
    
    def terminate(self):
        """Terminate all instances (compatible with old interface)"""
        logger.info("Terminating all instances...")
        result = self.deployer.cleanup(force=True, delete_images=False)
        
        success = sum(1 for v in result.get("instances", {}).values() if v)
        total = len(result.get("instances", {}))
        logger.info(f"Terminated {success}/{total} instances")
        
        return result
    
    def remote_execute(self, script_path: str, args: Optional[List[str]] = None):
        """Execute script on all instances (compatible with old interface)"""
        import os
        import shlex

        from conflux_deployer.utils.remote import RemoteExecutor
        
        if args is None:
            args = []
        
        script_name = os.path.basename(script_path)
        ssh_user = "ubuntu"  # Default user

        key_path = self.deployer.config.ssh_private_key_path
        executor = RemoteExecutor(ssh_key_path=key_path, ssh_user=ssh_user)

        remote_path = f"/tmp/{script_name}"
        put_results = executor.copy_file_to_all(self.ip_addresses, script_path, remote_path, max_workers=100, retry=2)

        quoted_args = " ".join(shlex.quote(a) for a in args)
        run_cmd = f"chmod +x {shlex.quote(remote_path)} && {shlex.quote(remote_path)} {quoted_args}".strip()

        exec_results = executor.execute_on_all(self.ip_addresses, run_cmd, max_workers=100, retry=1, timeout=1800)

        ok = 0
        for ip in self.ip_addresses:
            if put_results.get(ip) and exec_results.get(ip) and exec_results[ip].success:
                ok += 1
            else:
                err = exec_results[ip].stderr if ip in exec_results else ""
                logger.warning(f"Failed to execute on {ip}: {err}")

        logger.info(f"Executed on {ok}/{len(self.ip_addresses)} instances")
        return ok == len(self.ip_addresses)


class DeployerAdapter:
    """
    Adapter that provides a simple interface for the remote_simulation tests
    to use the new conflux_deployer framework.
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        state_path: Optional[str] = None,
    ):
        """
        Initialize the adapter.
        
        Args:
            config_path: Path to deployment config JSON file
            state_path: Path to state file for recovery
        """
        self.config_path = config_path
        self.state_path = state_path
        self._deployer: Optional[ConfluxDeployer] = None
        self._instances: Optional[List[InstanceInfo]] = None
    
    @property
    def deployer(self) -> ConfluxDeployer:
        if self._deployer is None:
            raise RuntimeError("Deployer not initialized. Call launch() or recover() first.")
        return self._deployer
    
    @property
    def instances(self) -> LegacyInstances:
        """Get instances in legacy-compatible format"""
        if self._instances is None:
            raise RuntimeError("No instances available. Call launch() or recover() first.")
        return LegacyInstances(deployer=self.deployer, instances=self._instances)
    
    @property
    def ip_addresses(self) -> List[str]:
        """Get IP addresses (shortcut for compatibility)"""
        return self.instances.ip_addresses
    
    def launch(
        self,
        instance_count: int,
        instance_type: str = "m6i.2xlarge",
        regions: Optional[List[Dict[str, Any]]] = None,
        aws_credentials: Optional[Dict[str, str]] = None,
        alibaba_credentials: Optional[Dict[str, str]] = None,
        pull_docker_image: bool = True,
    ) -> LegacyInstances:
        """
        Launch instances using the new framework.
        
        Args:
            instance_count: Total number of instances to launch
            instance_type: Instance type (default: m6i.2xlarge)
            regions: Optional list of region configs
            aws_credentials: AWS credentials dict
            alibaba_credentials: Alibaba credentials dict
            pull_docker_image: Whether to pull Docker image on startup
            
        Returns:
            LegacyInstances object compatible with old interface
        """
        import os
        from dotenv import load_dotenv
        load_dotenv()
        
        # Build credentials
        credentials = {}
        
        ssh_key_name: Optional[str] = None
        ssh_key_path: Optional[str] = None

        if aws_credentials:
            credentials[CloudProvider.AWS] = CloudCredentials(
                access_key_id=aws_credentials.get("access_key_id", ""),
                secret_access_key=aws_credentials.get("secret_access_key", ""),
                session_token=aws_credentials.get("session_token"),
            )
            # Optional keypair info
            ssh_key_name = aws_credentials.get("key_pair_name")
            ssh_key_path = aws_credentials.get("key_pair_path")
        else:
            # Try to load from environment
            aws_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
            aws_keypair = os.getenv("CONFLUX_MASSIVE_TEST_AWS_KEY_PAIR", "conflux-keypair")
            
            if aws_key and aws_secret:
                credentials[CloudProvider.AWS] = CloudCredentials(
                    access_key_id=aws_key,
                    secret_access_key=aws_secret,
                )
                ssh_key_name = aws_keypair
                ssh_key_path = f"~/.ssh/{aws_keypair}.pem"
        
        if alibaba_credentials:
            credentials[CloudProvider.ALIBABA] = CloudCredentials(
                access_key_id=alibaba_credentials.get("access_key_id", ""),
                secret_access_key=alibaba_credentials.get("secret_access_key", ""),
                session_token=alibaba_credentials.get("session_token"),
            )
            if not ssh_key_name:
                ssh_key_name = alibaba_credentials.get("key_pair_name")
            if not ssh_key_path:
                ssh_key_path = alibaba_credentials.get("key_pair_path")
        else:
            ali_key = os.getenv("ALIBABA_ACCESS_KEY_ID")
            ali_secret = os.getenv("ALIBABA_SECRET_ACCESS_KEY")
            ali_keypair = os.getenv("CONFLUX_MASSIVE_TEST_ALIBABA_KEY_PAIR", "conflux-keypair")
            
            if ali_key and ali_secret:
                credentials[CloudProvider.ALIBABA] = CloudCredentials(
                    access_key_id=ali_key,
                    secret_access_key=ali_secret,
                )
                if not ssh_key_name:
                    ssh_key_name = ali_keypair
                if not ssh_key_path:
                    ssh_key_path = f"~/.ssh/{ali_keypair}.pem"
        
        if not credentials:
            raise ValueError(
                "No credentials provided. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                "or ALIBABA_ACCESS_KEY_ID/ALIBABA_SECRET_ACCESS_KEY environment variables."
            )
        
        # Build region configs
        if regions:
            region_configs = [
                RegionConfig(
                    provider=CloudProvider(r.get("provider", "aws")),
                    region_id=r.get("region_id", "us-west-2"),
                    location_name=r.get("location_name", r.get("region_id", "us-west-2")),
                    instance_count=r.get("instance_count", 1),
                    nodes_per_instance=r.get("nodes_per_instance", 1),
                    instance_type=r.get("instance_type", instance_type),
                )
                for r in regions
            ]
        else:
            # Default: single region with all instances
            provider = CloudProvider.AWS if CloudProvider.AWS in credentials else CloudProvider.ALIBABA
            default_region = "us-west-2" if provider == CloudProvider.AWS else "cn-hangzhou"
            
            region_configs = [
                RegionConfig(
                    provider=provider,
                    region_id=default_region,
                    location_name=default_region,
                    instance_count=instance_count,
                    nodes_per_instance=1,
                    instance_type=instance_type,
                )
            ]
        
        # Create deployment config
        import datetime
        deployment_id = f"conflux-test-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        config = DeploymentConfig(
            deployment_id=deployment_id,
            instance_name_prefix="conflux-node",
            credentials=credentials,
            regions=region_configs,
            image=ImageConfig(
                image_name_prefix="conflux-node-image",
                conflux_docker_image="confluxchain/conflux-rust:latest",
            ),
            conflux_node=ConfluxNodeConfig(
                node_index=0,
                p2p_port_base=32323,
                jsonrpc_port_base=12537,
                chain_id=1,
            ),
            network=NetworkConfig(
                connect_peers=3,
                bandwidth_mbit=20,
                enable_tx_propagation=False,  # Use topology logic
            ),
            test=TestConfig(
                test_type="custom",
                num_blocks=0,
            ),
            cleanup=CleanupConfig(
                auto_terminate=False,  # Manual cleanup for compatibility
                delete_images=False,
            ),
            ssh_key_name=ssh_key_name,
            ssh_private_key_path=ssh_key_path,
        )
        
        # Create deployer and launch
        self._deployer = ConfluxDeployer(config)
        
        logger.info(f"Launching {instance_count} instances...")
        
        # Create images
        self._deployer.create_images()
        
        # Deploy servers
        self._instances = self._deployer.deploy_servers()
        
        logger.success(f"Launched {len(self._instances)} instances")
        logger.info(f"IP addresses: {self.ip_addresses}")
        
        return self.instances
    
    def recover(self, state_path: Optional[str] = None) -> LegacyInstances:
        """
        Recover from a previous deployment.
        
        Args:
            state_path: Path to state file
            
        Returns:
            LegacyInstances object
        """
        path = state_path or self.state_path
        if not path:
            raise ValueError("state_path is required for recovery")
        
        self._deployer = ConfluxDeployer.recover(path, self.config_path)
        self._instances = self._deployer.recover_deployment()
        
        logger.info(f"Recovered {len(self._instances)} instances")
        return self.instances
    
    def terminate(self):
        """Terminate all instances"""
        if self._deployer:
            return self._deployer.cleanup(force=True, delete_images=False)
        logger.warning("No deployer to terminate")
        return {}
    
    def save_state(self, path: Optional[str] = None):
        """Save current state for later recovery"""
        if self._deployer:
            self._deployer.state_manager.save()
            logger.info(f"State saved to {self._deployer.state_path}")
    
    def estimate_cost(self) -> Dict[str, float]:
        """Estimate running cost"""
        if self._deployer:
            return self._deployer.estimate_cost()
        return {"hourly_cost_usd": 0.0, "daily_cost_usd": 0.0}


def create_instances_from_deployer(
    instance_count: int,
    instance_type: str = "m6i.2xlarge",
    regions: Optional[List[Dict[str, Any]]] = None,
) -> LegacyInstances:
    """
    Convenience function to create instances using the new framework.
    
    This can be used as a drop-in replacement for the old launch flow.
    
    Args:
        instance_count: Number of instances
        instance_type: Instance type
        regions: Optional region configurations
        
    Returns:
        LegacyInstances compatible with old interface
    """
    adapter = DeployerAdapter()
    return adapter.launch(
        instance_count=instance_count,
        instance_type=instance_type,
        regions=regions,
    )


def recover_instances(state_path: str) -> LegacyInstances:
    """
    Recover instances from state file.
    
    Args:
        state_path: Path to state file
        
    Returns:
        LegacyInstances compatible with old interface
    """
    adapter = DeployerAdapter(state_path=state_path)
    return adapter.recover()
