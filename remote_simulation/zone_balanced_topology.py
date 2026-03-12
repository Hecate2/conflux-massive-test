"""Zone-balanced topology generator.

This strategy targets the failure mode observed in the large runs: hotspots form
when some nodes become both locally central and early receivers. The generator
therefore tries to keep the graph close to regular while spreading peers across
zones, instead of creating region hubs or asymmetric in/out concentration.

Design:
- Respect the undirected nature of ``NetworkTopology.add_connection``.
- Seed a small amount of same-zone connectivity for robustness.
- Fill the remaining degree budget with a zone-diverse regular-pairing process.
- Keep actual degree almost perfectly flat while preserving random-like
    expansion instead of a lattice.
"""
from __future__ import annotations

import random
from collections import defaultdict
from statistics import mean, pstdev
from typing import DefaultDict, Dict, Iterable, List

from loguru import logger

from remote_simulation.network_topology import NetworkTopology
from remote_simulation.remote_node import RemoteNode


def generate_zone_balanced_topology(
    nodes: List[RemoteNode],
    target_degree: int = 8,
    intra_zone_degree: int = 2,
    latency_min: int = 1,
    latency_max: int = 20,
    seed: int | None = None,
) -> NetworkTopology:
    """Generate a near-regular topology that balances degree across zones.

    Args:
        nodes: ordered node list used by the simulation.
        target_degree: desired undirected peer count per node.
        intra_zone_degree: desired same-zone peers per node when feasible.
        latency_min: minimum sampled per-edge latency.
        latency_max: maximum sampled per-edge latency.
        seed: optional seed for reproducible topology generation.
    """
    topology = NetworkTopology()
    num_nodes = len(nodes)
    if num_nodes <= 1:
        return topology

    if seed is not None:
        random.seed(seed)

    target_degree = max(1, min(target_degree, num_nodes - 1))
    groups = _group_nodes_by_zone(nodes)
    logger.info(
        "Generating zone-balanced topology for {} nodes across {} zones, target degree {}",
        num_nodes,
        len(groups),
        target_degree,
    )

    for idxs in groups.values():
        random.shuffle(idxs)

    _build_balanced_graph(
        topology,
        nodes,
        groups,
        target_degree,
        intra_zone_degree,
        latency_min,
        latency_max,
        seed,
    )
    _log_topology_summary(topology, nodes)
    return topology


def _group_nodes_by_zone(nodes: List[RemoteNode]) -> Dict[str, List[int]]:
    groups: DefaultDict[str, List[int]] = defaultdict(list)
    for idx, node in enumerate(nodes):
        zone = node.host_spec.zone or node.host_spec.region or node.host_spec.provider or "unknown"
        groups[zone].append(idx)
    return dict(groups)


def _degree(topology: NetworkTopology, node_idx: int) -> int:
    return len(topology.get_peers(node_idx))


def _sample_latency(latency_min: int, latency_max: int) -> int:
    return random.randint(latency_min, latency_max)


def _try_add_edge(
    topology: NetworkTopology,
    left: int,
    right: int,
    target_degree: int,
    latency_min: int,
    latency_max: int,
) -> bool:
    if left == right:
        return False
    if right in topology.get_peers(left):
        return False
    if _degree(topology, left) >= target_degree or _degree(topology, right) >= target_degree:
        return False
    topology.add_connection(left, right, _sample_latency(latency_min, latency_max))
    return True


def _add_intra_zone_edges(
    topology: NetworkTopology,
    groups: Dict[str, List[int]],
    target_degree: int,
    intra_zone_degree: int,
    latency_min: int,
    latency_max: int,
) -> None:
    for idxs in groups.values():
        size = len(idxs)
        if size <= 1:
            continue
        desired = min(intra_zone_degree, target_degree, size - 1)
        if desired <= 0:
            continue
        if size == 2:
            _try_add_edge(topology, idxs[0], idxs[1], target_degree, latency_min, latency_max)
            continue

        seen: set[tuple[int, int]] = set()
        for pos, left in enumerate(idxs):
            right = idxs[(pos + 1) % size]
            pair = (min(left, right), max(left, right))
            if pair in seen:
                continue
            seen.add(pair)
            _try_add_edge(topology, left, right, target_degree, latency_min, latency_max)


def _candidate_nodes_by_zone(groups: Dict[str, List[int]], exclude_zone: str | None = None) -> Iterable[int]:
    for zone, idxs in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        if zone == exclude_zone:
            continue
        for idx in idxs:
            yield idx


def _build_balanced_graph(
    topology: NetworkTopology,
    nodes: List[RemoteNode],
    groups: Dict[str, List[int]],
    target_degree: int,
    intra_zone_degree: int,
    latency_min: int,
    latency_max: int,
    seed: int | None,
) -> None:
    max_retries = 8
    for attempt in range(max_retries):
        if attempt > 0:
            topology.peers.clear()
        if seed is not None:
            random.seed(seed + attempt)
        _add_intra_zone_edges(topology, groups, target_degree, intra_zone_degree, latency_min, latency_max)
        if _fill_regular_zone_diverse_edges(topology, nodes, groups, target_degree, latency_min, latency_max):
            return
    raise RuntimeError("failed to build balanced topology after repeated attempts")


def _fill_regular_zone_diverse_edges(
    topology: NetworkTopology,
    nodes: List[RemoteNode],
    groups: Dict[str, List[int]],
    target_degree: int,
    latency_min: int,
    latency_max: int,
) -> bool:
    remaining = [target_degree - _degree(topology, idx) for idx in range(len(nodes))]
    if any(value < 0 for value in remaining):
        return False

    max_steps = len(nodes) * target_degree * 12
    for _ in range(max_steps):
        active = [idx for idx, value in enumerate(remaining) if value > 0]
        if not active:
            return True

        active.sort(key=lambda idx: (-remaining[idx], _degree(topology, idx), random.random()))
        source = active[0]
        source_zone = nodes[source].host_spec.zone or nodes[source].host_spec.region or "unknown"
        source_peer_zones = {nodes[peer].host_spec.zone or nodes[peer].host_spec.region or "unknown" for peer in topology.get_peers(source)}

        candidates = [
            cand
            for cand in active[1:]
            if cand not in topology.get_peers(source)
        ]
        if not candidates:
            return False

        def candidate_score(candidate: int) -> tuple[int, int, int, float]:
            candidate_zone = nodes[candidate].host_spec.zone or nodes[candidate].host_spec.region or "unknown"
            cross_zone = 1 if candidate_zone != source_zone else 0
            new_zone = 1 if candidate_zone not in source_peer_zones else 0
            return (
                cross_zone,
                new_zone,
                remaining[candidate],
                -random.random(),
            )

        candidates.sort(key=candidate_score, reverse=True)
        selected = None
        for candidate in candidates[:32]:
            if _try_add_edge(topology, source, candidate, target_degree, latency_min, latency_max):
                selected = candidate
                break

        if selected is None:
            return False

        remaining[source] -= 1
        remaining[selected] -= 1

    return all(value == 0 for value in remaining)


def _two_hop_reach(topology: NetworkTopology, node_idx: int) -> int:
    one_hop = topology.get_peers(node_idx)
    two_hop: set[int] = set()
    for peer in one_hop:
        two_hop.update(topology.get_peers(peer))
    two_hop.discard(node_idx)
    return len(two_hop)


def _log_topology_summary(topology: NetworkTopology, nodes: List[RemoteNode]) -> None:
    if not nodes:
        return
    degrees = [_degree(topology, idx) for idx in range(len(nodes))]
    two_hops = [_two_hop_reach(topology, idx) for idx in range(len(nodes))]
    logger.info(
        "Zone-balanced topology generated: degree avg={:.2f}, std={:.2f}, min={}, max={}; two-hop avg={:.2f}, std={:.2f}, min={}, max={}",
        mean(degrees),
        pstdev(degrees) if len(degrees) > 1 else 0.0,
        min(degrees),
        max(degrees),
        mean(two_hops),
        pstdev(two_hops) if len(two_hops) > 1 else 0.0,
        min(two_hops),
        max(two_hops),
    )


__all__ = ["generate_zone_balanced_topology"]