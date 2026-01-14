import json
from pathlib import Path

import pytest

from conflux_deployer.configs.loader import StateManager
from conflux_deployer.configs.types import CloudProvider, InstanceInfo, InstanceState, DeploymentState


def _mk_instance(*, instance_id: str = "i-1") -> InstanceInfo:
    return InstanceInfo(
        instance_id=instance_id,
        provider=CloudProvider.AWS,
        region_id="us-west-2",
        location_name="us-west-2",
        instance_type="m6i.2xlarge",
        public_ip="1.2.3.4",
        private_ip="10.0.0.1",
        state=InstanceState.RUNNING,
        nodes_count=2,
        name="unit-test-instance",
        launch_time="2026-01-01T00:00:00",
        metadata={"k": "v"},
    )


def test_initialize_creates_file_and_load_roundtrips(tmp_path: Path):
    state_path = tmp_path / "state.json"
    sm = StateManager(str(state_path))
    st = sm.initialize("deploy-1")

    assert state_path.exists()
    assert st.deployment_id == "deploy-1"
    assert st.phase == "initialized"

    sm2 = StateManager(str(state_path))
    loaded = sm2.load()
    assert loaded is not None
    assert loaded.deployment_id == "deploy-1"


def test_add_update_remove_instance_persists(tmp_path: Path):
    state_path = tmp_path / "state.json"
    sm = StateManager(str(state_path))
    sm.initialize("deploy-2")

    inst = _mk_instance(instance_id="i-abc")
    sm.add_instance(inst)

    sm2 = StateManager(str(state_path))
    loaded = sm2.load()
    assert loaded is not None
    assert len(loaded.instances) == 1
    assert loaded.instances[0].instance_id == "i-abc"

    # Update and re-load
    sm2.update_instance("i-abc", public_ip="5.6.7.8", state=InstanceState.STOPPED)

    sm3 = StateManager(str(state_path))
    loaded2 = sm3.load()
    assert loaded2 is not None
    assert loaded2.instances[0].public_ip == "5.6.7.8"
    assert loaded2.instances[0].state == InstanceState.STOPPED

    # Remove and re-load
    sm3.remove_instance("i-abc")
    sm4 = StateManager(str(state_path))
    loaded3 = sm4.load()
    assert loaded3 is not None
    assert loaded3.instances == []


def test_add_image_and_errors_and_results_persist(tmp_path: Path):
    state_path = tmp_path / "state.json"
    sm = StateManager(str(state_path))
    sm.initialize("deploy-3")

    sm.add_image("aws", "us-west-2", "ami-123")
    sm.add_error("boom")
    sm.set_test_results({"ok": True, "n": 1})

    sm2 = StateManager(str(state_path))
    loaded = sm2.load()
    assert loaded is not None
    assert loaded.images["aws"]["us-west-2"] == "ami-123"
    assert any("boom" in e for e in loaded.errors)
    assert loaded.test_results == {"ok": True, "n": 1}


def test_invalid_json_state_file_raises_value_error(tmp_path: Path):
    state_path = tmp_path / "state.json"
    state_path.write_text("{not-json}")

    sm = StateManager(str(state_path))
    with pytest.raises(ValueError):
        sm.load()


def test_deployment_state_to_from_dict_roundtrip():
    ds = DeploymentState(
        deployment_id="deploy-rt",
        phase="nodes_started",
        images={"aws": {"us-west-2": "ami-xyz"}},
        instances=[_mk_instance(instance_id="i-rt")],
        nodes=[{"node_id": "n1"}],
        test_results={"latency_ms": 12.3},
        errors=["e"],
        created_at="t1",
        updated_at="t2",
    )

    d = ds.to_dict()
    ds2 = DeploymentState.from_dict(d)

    assert ds2.deployment_id == ds.deployment_id
    assert ds2.phase == ds.phase
    assert ds2.images == ds.images
    assert len(ds2.instances) == 1
    assert ds2.instances[0].instance_id == "i-rt"
    assert ds2.test_results == ds.test_results
