import json
from pathlib import Path

import pytest

from conflux_deployer.configs.loader import ConfigLoader
from conflux_deployer.configs.types import CloudProvider


def _minimal_config_dict(*, include_deployment_id: bool = False):
    data = {
        "instance_name_prefix": "unit-test",
        "credentials": {
            "aws": {
                "access_key_id": "",
                "secret_access_key": "",
            }
        },
        "regions": [
            {
                "provider": "aws",
                "region_id": "us-west-2",
                "location_name": "us-west-2",
                "instance_count": 2,
                "instance_type": "m6i.2xlarge",
                "nodes_per_instance": 1,
            }
        ],
        "image": {
            "image_name_prefix": "conflux-test-node",
            "ubuntu_version": "22.04",
        },
        "conflux_node": {
            "p2p_port_base": 32323,
            "jsonrpc_port_base": 12537,
        },
        "network": {
            "connect_peers": 3,
        },
        "test": {
            "test_type": "stress",
        },
        "cleanup": {
            "auto_terminate": True,
        },
    }
    if include_deployment_id:
        data["deployment_id"] = "deploy-unit-test"
    return data


def test_load_from_file_missing_raises(tmp_path: Path):
    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        ConfigLoader.load_from_file(str(missing))


def test_from_dict_autogenerates_deployment_id_when_missing():
    cfg = ConfigLoader.from_dict(_minimal_config_dict(include_deployment_id=False))
    assert cfg.deployment_id is not None
    assert cfg.deployment_id.startswith("deploy-")


def test_from_dict_parses_credentials_and_regions():
    cfg = ConfigLoader.from_dict(_minimal_config_dict(include_deployment_id=True))

    assert cfg.deployment_id == "deploy-unit-test"
    assert cfg.instance_name_prefix == "unit-test"

    assert CloudProvider.AWS in cfg.credentials
    assert cfg.credentials[CloudProvider.AWS].access_key_id == ""

    assert len(cfg.regions) == 1
    r0 = cfg.regions[0]
    assert r0.provider == CloudProvider.AWS
    assert r0.region_id == "us-west-2"
    assert r0.instance_count == 2
    assert r0.total_nodes() == 2


def test_save_to_file_creates_directories_and_roundtrips(tmp_path: Path):
    cfg = ConfigLoader.from_dict(_minimal_config_dict(include_deployment_id=True))

    out_path = tmp_path / "nested" / "config.json"
    ConfigLoader.save_to_file(cfg, str(out_path))

    assert out_path.exists()

    loaded = ConfigLoader.load_from_file(str(out_path))
    assert loaded.deployment_id == cfg.deployment_id
    assert loaded.instance_name_prefix == cfg.instance_name_prefix
    assert len(loaded.regions) == len(cfg.regions)
    assert loaded.regions[0].instance_type == cfg.regions[0].instance_type


def test_invalid_provider_raises_value_error():
    data = _minimal_config_dict(include_deployment_id=True)
    data["regions"][0]["provider"] = "not-a-provider"

    with pytest.raises(ValueError):
        ConfigLoader.from_dict(data)


def test_to_dict_uses_provider_value_keys():
    cfg = ConfigLoader.from_dict(_minimal_config_dict(include_deployment_id=True))
    d = ConfigLoader.to_dict(cfg)

    assert "credentials" in d
    assert "aws" in d["credentials"]
    assert "alibaba" not in d["credentials"]
