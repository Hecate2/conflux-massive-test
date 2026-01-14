from __future__ import annotations

from typing import Dict

import conflux.config as legacy_conflux_config
from remote_simulation.config_builder import (
    SimulateOptions,
    ConfluxOptions,
    _generate_config_dict,
    _normalize_config_value,
)

from conflux_deployer.configs.types import CloudProvider, ConfluxNodeConfig, DeploymentConfig, InstanceInfo, NetworkConfig, NodeInfo
from conflux_deployer.node_management.manager import NodeManager


def _parse_flat_toml(toml_text: str) -> Dict[str, str]:
    """Parse the simple key/value TOML we generate (no sections/arrays).

    Returns a map of key -> raw value string (e.g. '"test"', 'true', '123').
    """
    out: Dict[str, str] = {}
    for raw_line in toml_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def test_generate_node_config_matches_remote_simulation_defaults():
    sim = SimulateOptions()
    node_opts = ConfluxOptions()
    legacy = _generate_config_dict(sim, node_opts)

    # Build a deployer node that matches the legacy node-0 port layout.
    p2p = int(legacy["tcp_port"])
    jsonrpc_http = int(legacy["jsonrpc_http_port"])

    cfg = DeploymentConfig(
        conflux_node=ConfluxNodeConfig(
            node_index=0,
            p2p_port_base=p2p,
            jsonrpc_port_base=jsonrpc_http,
            storage_memory_gb=sim.storage_memory_gb,
            tx_pool_size=node_opts.tx_pool_size,
            chain_id=legacy_conflux_config.DEFAULT_PY_TEST_CHAIN_ID,
        ),
        network=NetworkConfig(target_tps=sim.target_tps),
    )

    inst = InstanceInfo(
        instance_id="i-1",
        provider=CloudProvider.AWS,
        region_id="us-test-1",
        location_name="us-test-1",
        instance_type="t3.large",
        public_ip="127.0.0.1",
        nodes_count=1,
    )

    node = NodeInfo(
        node_id="node-0",
        instance_info=inst,
        node_index=0,
        p2p_port=p2p,
        jsonrpc_port=jsonrpc_http,
    )

    nm = NodeManager(cfg, [inst], ssh_key_path=None)
    toml_cfg = nm.generate_node_config(
        node,
        enable_tx_gen=True,
        tx_gen_period_us=int(legacy["generate_tx_period_us"]),
    )

    toml_map = _parse_flat_toml(toml_cfg.to_toml())

    # Assert a representative set of keys matches legacy defaults.
    keys_to_check = [
        # Basics
        "mode",
        "chain_id",
        # Ports
        "tcp_port",
        "jsonrpc_http_port",
        "jsonrpc_ws_port",
        "jsonrpc_local_http_port",
        "jsonrpc_http_eth_port",
        "jsonrpc_ws_eth_port",
        # Performance/storage (scaled by storage_memory_gb)
        "db_cache_size",
        "ledger_cache_size",
        "storage_delta_mpts_cache_size",
        "storage_delta_mpts_cache_start_size",
        "storage_delta_mpts_slab_idle_size",
        "tx_pool_size",
        # Tx generation
        "generate_tx",
        "generate_tx_period_us",
        "genesis_secrets",
        "txgen_account_count",
        "txgen_batch_size",
        "send_tx_period_ms",
        # Misc toggles
        "persist_tx_index",
        "persist_block_number_index",
        "enable_optimistic_execution",
        "enable_discovery",
        "metrics_enabled",
        "rpc_enable_metrics",
        # Logging / APIs
        "log_level",
        "log_file",
        "metrics_output_file",
        "public_rpc_apis",
        # Gas/block settings
        "max_block_size_in_bytes",
        "execution_prefetch_threads",
        "target_block_gas_limit",
    ]

    for key in keys_to_check:
        assert key in legacy, f"legacy config missing key: {key}"
        assert key in toml_map, f"toml missing key: {key}"
        assert toml_map[key] == _normalize_config_value(legacy[key]), (
            f"mismatch for {key}: toml={toml_map[key]} legacy={_normalize_config_value(legacy[key])}"
        )
