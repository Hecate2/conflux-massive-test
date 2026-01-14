"""Example script for deploying Conflux network and running tests"""

import sys
import os
from typing import List, Dict, Any

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from conflux_deployer import ConfluxDeployer
from loguru import logger


def setup_logger():
    """Setup logger configuration"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO"
    )
    logger.add(
        "deploy_and_test.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="10 MB"
    )


def main():
    """Main function"""
    setup_logger()
    
    # Configuration
    config_path = "config.example.json"
    
    try:
        # Initialize deployer
        deployer = ConfluxDeployer(config_path)
        
        # Register signal handlers for cleanup
        deployer.resource_cleanup_manager.register_signal_handlers()
        
        # Deploy network across regions
        region_configs = [
            {
                "cloud_provider": "aws",
                "region": "us-west-2",
                "instance_type": "m6i.2xlarge",
                "count": 2
            },
            {
                "cloud_provider": "aws",
                "region": "us-east-1",
                "instance_type": "m7i.2xlarge",
                "count": 2
            },
            {
                "cloud_provider": "alibaba",
                "region": "us-west-1",
                "instance_type": "ecs.c6.2xlarge",
                "count": 2
            }
        ]
        
        logger.info("Deploying Conflux network across regions...")
        node_info_list = deployer.deploy_network(region_configs)
        
        # Run tests
        test_configs = [
            {
                "test_type": "tps",
                "config": {
                    "duration": 300,
                    "tx_rate": 1000
                }
            },
            {
                "test_type": "latency",
                "config": {
                    "duration": 300,
                    "tx_count": 1000
                }
            },
            {
                "test_type": "stability",
                "config": {
                    "duration": 600  # 10 minutes for testing
                }
            },
            {
                "test_type": "stress",
                "config": {
                    "duration": 600,
                    "max_tx_rate": 5000
                }
            }
        ]
        
        for test in test_configs:
            logger.info(f"Running {test['test_type']} test...")
            result = deployer.run_test(test["test_type"], test["config"])
            logger.info(f"Test result: {result}")
        
        # Schedule cleanup (optional)
        deployer.resource_cleanup_manager.schedule_cleanup(delay_seconds=3600)
        
        # Keep the script running to monitor
        logger.info("Deployment and testing completed. Press Ctrl+C to cleanup and exit.")
        while True:
            pass
            
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, starting cleanup...")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        # Always cleanup resources
        try:
            logger.info("Cleaning up resources...")
            cleanup_result = deployer.cleanup(force=True)
            logger.info(f"Cleanup result: {cleanup_result}")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")


if __name__ == "__main__":
    main()
