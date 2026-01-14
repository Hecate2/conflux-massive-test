"""
Conflux Deployer Framework

A Python framework for deploying and testing Conflux blockchain nodes
across multiple cloud providers (AWS and Alibaba Cloud).

Features:
- Multi-cloud deployment (AWS EC2, Alibaba Cloud ECS)
- Server image management with Docker and Conflux pre-installed
- Automatic instance state tracking and recovery
- Multiple Conflux nodes per high-spec instance
- Comprehensive test suite (stress, latency, fork tests)
- Automatic resource cleanup to prevent cost overruns
- Full type annotations for all configurations

Usage:
    from conflux_deployer import ConfluxDeployer
    
    deployer = ConfluxDeployer.from_config_file("config.json")
    
    with deployer.auto_cleanup():
        deployer.deploy_all()
        results = deployer.run_all_tests()

CLI:
    conflux-deployer deploy -c config.json
    conflux-deployer test all -c config.json
    conflux-deployer cleanup -c config.json
"""

__version__ = "0.1.0"

# Core types
from .configs import (
    CloudProvider,
    InstanceState,
    CloudCredentials,
    RegionConfig,
    InstanceTypeSpec,
    ImageConfig,
    ConfluxNodeConfig,
    NetworkConfig,
    TestConfig,
    CleanupConfig,
    DeploymentConfig,
    InstanceInfo,
    NodeInfo,
    DeploymentState,
)

# Main orchestrator
from .main import ConfluxDeployer, deploy_and_test

# Cloud providers
from .cloud import (
    CloudProviderBase,
    AWSProvider,
    AlibabaProvider,
    CloudProviderFactory,
    get_cloud_factory,
    get_provider,
)

# Managers
from .image_management import ImageManager
from .server_deployment import ServerDeployer
from .node_management import NodeManager, ConfluxRpcClient
from .test_control import TestController, TestResult
from .resource_cleanup import ResourceCleanupManager

# Configuration utilities
from .configs.loader import ConfigLoader, StateManager

__all__ = [
    # Version
    "__version__",
    # Types
    "CloudProvider",
    "InstanceState",
    "CloudCredentials",
    "RegionConfig",
    "InstanceTypeSpec",
    "ImageConfig",
    "ConfluxNodeConfig",
    "NetworkConfig",
    "TestConfig",
    "CleanupConfig",
    "DeploymentConfig",
    "InstanceInfo",
    "NodeInfo",
    "DeploymentState",
    # Main
    "ConfluxDeployer",
    "deploy_and_test",
    # Cloud
    "CloudProviderBase",
    "AWSProvider",
    "AlibabaProvider",
    "CloudProviderFactory",
    "get_cloud_factory",
    "get_provider",
    # Managers
    "ImageManager",
    "ServerDeployer",
    "NodeManager",
    "ConfluxRpcClient",
    "TestController",
    "TestResult",
    "ResourceCleanupManager",
    # Utils
    "ConfigLoader",
    "StateManager",
    # Adapter for legacy code
    "DeployerAdapter",
    "LegacyInstances",
    "create_instances_from_deployer",
    "recover_instances",
]

# Adapter imports (for backward compatibility with remote_simulation)
from .adapter import (
    DeployerAdapter,
    LegacyInstances,
    create_instances_from_deployer,
    recover_instances,
)
