"""
Configuration Module

Provides configuration types and loading utilities.
"""

from .types import (
    CloudProvider,
    InstanceState,
    CloudCredentials,
    RegionConfig,
    InstanceTypeSpec,
    AWS_INSTANCE_SPECS,
    ALIBABA_INSTANCE_SPECS,
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

from .loader import ConfigLoader, StateManager

__all__ = [
    # Enums
    "CloudProvider",
    "InstanceState",
    # Config types
    "CloudCredentials",
    "RegionConfig",
    "InstanceTypeSpec",
    "AWS_INSTANCE_SPECS",
    "ALIBABA_INSTANCE_SPECS",
    "ImageConfig",
    "ConfluxNodeConfig",
    "NetworkConfig",
    "TestConfig",
    "CleanupConfig",
    "DeploymentConfig",
    # Runtime types
    "InstanceInfo",
    "NodeInfo",
    "DeploymentState",
    # Utilities
    "ConfigLoader",
    "StateManager",
]
