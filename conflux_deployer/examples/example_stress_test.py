"""
Example: Deploy and Run Stress Test

This example demonstrates how to deploy Conflux nodes and run a stress test
programmatically using the ConfluxDeployer framework.
"""

import json
from pathlib import Path

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
)


def create_config_programmatically() -> DeploymentConfig:
    """Create a deployment configuration programmatically"""
    
    return DeploymentConfig(
        deployment_id="stress-test-example",
        instance_name_prefix="conflux-stress",
        
        credentials={
            CloudProvider.AWS: CloudCredentials(
                access_key_id="YOUR_AWS_KEY",
                secret_access_key="YOUR_AWS_SECRET",
            ),
        },
        # Optionally set SSH key/paths via DeploymentConfig.ssh_key_name / ssh_private_key_path if needed
        
        regions=[
            RegionConfig(
                provider=CloudProvider.AWS,
                region_id="us-west-2",
                location_name="US West (Oregon)",
                instance_count=3,
                nodes_per_instance=1,
                instance_type="m6i.2xlarge",
            ),
            RegionConfig(
                provider=CloudProvider.AWS,
                region_id="us-east-1",
                location_name="US East (N. Virginia)",
                instance_count=3,
                nodes_per_instance=1,
                instance_type="m6i.2xlarge",
            ),
        ],
        
        image=ImageConfig(
            image_name_prefix="conflux-stress-image",
            conflux_docker_image="confluxchain/conflux-rust:latest",
        ),
        
        conflux_node=ConfluxNodeConfig(
            node_index=0,
            chain_id=1,
        ),
        
        network=NetworkConfig(
            connect_peers=3,
            bandwidth_mbit=20,
            enable_tx_propagation=True,
        ),
        
        test=TestConfig(
            test_type="stress",
            num_blocks=1000,
            txs_per_block=1,
        ),
        
        cleanup=CleanupConfig(
            auto_terminate=True,
            delete_images=False,
        ),
    )


def run_from_config_file():
    """Run deployment from a configuration file"""
    
    # Load from JSON file
    config_path = Path(__file__).parent / "stress_test_config.json"
    
    # Create deployer
    deployer = ConfluxDeployer.from_config_file(str(config_path))
    
    # Use auto_cleanup context manager to ensure resources are cleaned up
    with deployer.auto_cleanup():
        # Deploy
        print("Deploying servers and nodes...")
        deploy_result = deployer.deploy_all()
        print(f"Deployed {len(deploy_result['instances'])} instances")
        print(f"Started {len(deploy_result['nodes'])} nodes")
        
        # Run stress test
        print("\nRunning stress test...")
        test_result = deployer.run_stress_test(duration_seconds=300)
        
        if test_result.success:
            print("Stress test PASSED!")
            # TestResult stores node metrics and aggregated fields
            print(f"Node metrics: {json.dumps(test_result.node_metrics, indent=2)}")
        else:
            print("Stress test FAILED!")
            print(f"Errors: {test_result.errors}")
        
        # Collect final metrics
        print("\nFinal metrics:")
        metrics = deployer.collect_metrics()
        print(json.dumps(metrics, indent=2, default=str))
    
    # Resources are automatically cleaned up when exiting the context


def run_programmatic():
    """Run deployment with programmatic configuration"""
    
    config = create_config_programmatically()
    deployer = ConfluxDeployer(config)
    
    try:
        # Step-by-step deployment for more control
        
        # 1. Create images (if needed)
        print("Creating server images...")
        images = deployer.create_images()
        print(f"Images ready: {images}")
        
        # 2. Deploy servers
        print("\nDeploying servers...")
        instances = deployer.deploy_servers()
        print(f"Deployed {len(instances)} instances")
        
        # 3. Initialize and start nodes
        print("\nInitializing nodes...")
        nodes = deployer.initialize_nodes(instances)
        print(f"Initialized {len(nodes)} nodes")
        
        deployer.start_nodes()
        
        # 4. Wait for nodes to be ready
        print("\nWaiting for nodes to be ready...")
        if not deployer.wait_for_nodes_ready(timeout=300):
            raise RuntimeError("Nodes failed to become ready")
        
        # 5. Connect nodes
        print("\nConnecting nodes...")
        peer_counts = deployer.connect_nodes()
        print(f"Peer counts: {peer_counts}")
        
        # 6. Run test
        print("\nRunning stress test...")
        result = deployer.run_stress_test(300)
        
        print(f"\nTest result: {'PASSED' if result.success else 'FAILED'}")
        # Support both legacy and new TestResult fields
        metrics_obj = getattr(result, 'metrics', getattr(result, 'node_metrics', {}))
        if metrics_obj:
            print(f"Metrics: {json.dumps(metrics_obj, indent=2)}")
        
    finally:
        # Always cleanup
        print("\nCleaning up...")
        deployer.cleanup()
        print("Cleanup completed")


def quick_deploy_and_test():
    """Quick one-liner to deploy and test"""
    from conflux_deployer import deploy_and_test
    
    config_path = Path(__file__).parent / "stress_test_config.json"
    result = deploy_and_test(str(config_path), auto_cleanup=True)
    
    print(f"Success: {result['success']}")
    return result


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "file":
            run_from_config_file()
        elif mode == "programmatic":
            run_programmatic()
        elif mode == "quick":
            quick_deploy_and_test()
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python example_stress_test.py [file|programmatic|quick]")
    else:
        print("Usage: python example_stress_test.py [file|programmatic|quick]")
        print("  file         - Run from config file")
        print("  programmatic - Run with programmatic config")
        print("  quick        - Quick one-liner")
