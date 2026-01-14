"""
Test Controller Module

Manages execution of various test scenarios on the Conflux network:
- Stress tests (block generation, transaction throughput)
- Latency tests (confirmation latency measurement)
- Fork tests (fork handling)
- Custom tests
"""

import time
import random
import threading
from typing import Dict, List, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod

from loguru import logger

from ..configs import DeploymentConfig, TestConfig, NodeInfo
from ..node_management import NodeManager, ConfluxRpcClient


@dataclass
class TestResult:
    """Result of a test execution"""
    test_type: str
    success: bool
    start_time: str
    end_time: str
    duration_seconds: float
    # Metrics
    blocks_generated: int = 0
    transactions_sent: int = 0
    average_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    min_latency_ms: float = 0.0
    throughput_tps: float = 0.0
    # Node metrics
    node_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Errors
    errors: List[str] = field(default_factory=list)
    # Additional data
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_type": self.test_type,
            "success": self.success,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "blocks_generated": self.blocks_generated,
            "transactions_sent": self.transactions_sent,
            "average_latency_ms": self.average_latency_ms,
            "max_latency_ms": self.max_latency_ms,
            "min_latency_ms": self.min_latency_ms,
            "throughput_tps": self.throughput_tps,
            "node_metrics": self.node_metrics,
            "errors": self.errors,
            "extra": self.extra,
        }


@dataclass
class BlockConfirmationInfo:
    """Tracks block confirmation latency"""
    block_hash: str
    generation_time: float
    confirmation_time: Optional[float] = None
    
    @property
    def latency_ms(self) -> Optional[float]:
        if self.confirmation_time:
            return (self.confirmation_time - self.generation_time) * 1000
        return None


class BaseTest(ABC):
    """Base class for all test implementations"""
    
    def __init__(
        self,
        node_manager: NodeManager,
        config: TestConfig,
    ):
        self.node_manager = node_manager
        self.config = config
        self._stopped = False
    
    @abstractmethod
    def run(self) -> TestResult:
        """Run the test"""
        pass
    
    def stop(self) -> None:
        """Stop the test"""
        self._stopped = True


class StressTest(BaseTest):
    """
    Stress test - generates blocks and measures throughput.
    
    This test:
    1. Generates blocks at a configurable rate
    2. Tracks confirmation latency for each block
    3. Measures overall throughput
    """
    
    CONFIRMATION_THRESHOLD = 0.1**6 * 2**256
    
    def __init__(
        self,
        node_manager: NodeManager,
        config: TestConfig,
    ):
        super().__init__(node_manager, config)
        self._confirmation_info: Dict[str, BlockConfirmationInfo] = {}
        self._confirmed_blocks: List[BlockConfirmationInfo] = []
        self._lock = threading.Lock()
    
    def run(self) -> TestResult:
        """Run the stress test"""
        start_time = time.time()
        start_time_str = datetime.now().isoformat()
        
        errors: List[str] = []
        blocks_generated = 0
        
        nodes = self.node_manager.nodes
        if not nodes:
            return TestResult(
                test_type="stress",
                success=False,
                start_time=start_time_str,
                end_time=datetime.now().isoformat(),
                duration_seconds=0,
                errors=["No nodes available"],
            )
        
        num_nodes = len(nodes)
        
        # Start confirmation monitoring thread
        monitor_thread = threading.Thread(
            target=self._monitor_confirmations,
            daemon=True,
        )
        monitor_thread.start()
        
        # Generate blocks
        logger.info(f"Starting stress test: {self.config.num_blocks} blocks")
        
        generation_period = self.config.custom_params.get(
            "generation_period_ms", 
            500
        ) / 1000.0
        
        try:
            for i in range(1, self.config.num_blocks + 1):
                if self._stopped:
                    break
                
                wait_sec = random.expovariate(1.0 / generation_period)
                
                # Select a random node
                node = random.choice(nodes)
                client = self.node_manager.get_rpc_client(node)
                
                try:
                    # Generate block
                    block_hash = client.generate_one_block(
                        self.config.txs_per_block,
                        self.config.custom_params.get("max_block_size", 300000),
                    )
                    
                    # Track for confirmation
                    with self._lock:
                        self._confirmation_info[block_hash] = BlockConfirmationInfo(
                            block_hash=block_hash,
                            generation_time=time.time(),
                        )
                    
                    blocks_generated += 1
                    
                    if i % self.config.report_interval == 0:
                        confirmed = len(self._confirmed_blocks)
                        logger.info(
                            f"[PROGRESS] {i} blocks generated, "
                            f"{confirmed} confirmed"
                        )
                        
                except Exception as e:
                    errors.append(f"Block generation error on {node.node_id}: {e}")
                    logger.warning(f"Block generation error: {e}")
                
                # Wait for next block
                time.sleep(wait_sec)
                
        except Exception as e:
            errors.append(f"Test error: {e}")
            logger.error(f"Test error: {e}")
        
        # Stop monitoring
        self._stopped = True
        time.sleep(2)  # Give monitor time to finish
        
        # Calculate metrics
        end_time = time.time()
        duration = end_time - start_time
        
        latencies = [
            info.latency_ms 
            for info in self._confirmed_blocks 
            if info.latency_ms is not None
        ]
        
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0
        min_latency = min(latencies) if latencies else 0
        
        throughput = blocks_generated / duration if duration > 0 else 0
        
        # Collect node metrics
        node_metrics = self.node_manager.collect_metrics()
        
        return TestResult(
            test_type="stress",
            success=len(errors) == 0,
            start_time=start_time_str,
            end_time=datetime.now().isoformat(),
            duration_seconds=duration,
            blocks_generated=blocks_generated,
            transactions_sent=blocks_generated * self.config.txs_per_block,
            average_latency_ms=avg_latency,
            max_latency_ms=max_latency,
            min_latency_ms=min_latency,
            throughput_tps=throughput,
            node_metrics=node_metrics,
            errors=errors,
            extra={
                "confirmed_blocks": len(self._confirmed_blocks),
                "unconfirmed_blocks": len(self._confirmation_info),
            },
        )
    
    def _monitor_confirmations(self) -> None:
        """Monitor block confirmations in background"""
        nodes = self.node_manager.nodes
        
        while not self._stopped:
            if not nodes:
                time.sleep(1)
                continue
            
            with self._lock:
                pending_hashes = list(self._confirmation_info.keys())
            
            if not pending_hashes:
                time.sleep(0.5)
                continue
            
            # Check confirmations
            for block_hash in pending_hashes[:80]:  # Check up to 80 at a time
                try:
                    node = random.choice(nodes)
                    client = self.node_manager.get_rpc_client(node)
                    
                    risk = client.get_confirmation_risk(block_hash)
                    
                    if risk >= 0 and risk <= self.CONFIRMATION_THRESHOLD:
                        with self._lock:
                            if block_hash in self._confirmation_info:
                                info = self._confirmation_info.pop(block_hash)
                                info.confirmation_time = time.time()
                                self._confirmed_blocks.append(info)
                                
                except Exception:
                    pass
            
            time.sleep(0.5)


class LatencyTest(BaseTest):
    """
    Latency test - measures block confirmation latency across regions.
    
    This test:
    1. Generates blocks from different nodes
    2. Measures confirmation latency from each region
    3. Reports latency distribution
    """
    
    def run(self) -> TestResult:
        """Run the latency test"""
        start_time = time.time()
        start_time_str = datetime.now().isoformat()
        
        nodes = self.node_manager.nodes
        errors: List[str] = []
        latencies_by_region: Dict[str, List[float]] = {}
        
        if not nodes:
            return TestResult(
                test_type="latency",
                success=False,
                start_time=start_time_str,
                end_time=datetime.now().isoformat(),
                duration_seconds=0,
                errors=["No nodes available"],
            )
        
        # Initialize region tracking
        for node in nodes:
            region = node.instance_info.location_name
            if region not in latencies_by_region:
                latencies_by_region[region] = []
        
        num_samples = self.config.custom_params.get("num_samples", 100)
        
        logger.info(f"Starting latency test: {num_samples} samples")
        
        for i in range(num_samples):
            if self._stopped:
                break
            
            # Generate block from random node
            source_node = random.choice(nodes)
            source_client = self.node_manager.get_rpc_client(source_node)
            
            try:
                gen_time = time.time()
                block_hash = source_client.generate_one_block()
                
                # Wait and check confirmation from each region
                time.sleep(1)
                
                for node in nodes:
                    region = node.instance_info.location_name
                    client = self.node_manager.get_rpc_client(node)
                    
                    try:
                        # Try to get block
                        block = client.get_block_by_hash(block_hash)
                        
                        if block:
                            latency = (time.time() - gen_time) * 1000
                            latencies_by_region[region].append(latency)
                    except Exception:
                        pass
                
                if (i + 1) % 10 == 0:
                    logger.info(f"[PROGRESS] {i + 1}/{num_samples} samples")
                    
            except Exception as e:
                errors.append(f"Sample {i} error: {e}")
            
            time.sleep(0.5)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate metrics
        all_latencies = []
        for latencies in latencies_by_region.values():
            all_latencies.extend(latencies)
        
        avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else 0
        max_latency = max(all_latencies) if all_latencies else 0
        min_latency = min(all_latencies) if all_latencies else 0
        
        # Calculate per-region stats
        region_stats = {}
        for region, latencies in latencies_by_region.items():
            if latencies:
                region_stats[region] = {
                    "avg_ms": sum(latencies) / len(latencies),
                    "max_ms": max(latencies),
                    "min_ms": min(latencies),
                    "samples": len(latencies),
                }
        
        return TestResult(
            test_type="latency",
            success=len(errors) < num_samples * 0.1,  # Allow 10% errors
            start_time=start_time_str,
            end_time=datetime.now().isoformat(),
            duration_seconds=duration,
            blocks_generated=num_samples,
            average_latency_ms=avg_latency,
            max_latency_ms=max_latency,
            min_latency_ms=min_latency,
            node_metrics=self.node_manager.collect_metrics(),
            errors=errors,
            extra={"latencies_by_region": region_stats},
        )


class ForkTest(BaseTest):
    """
    Fork test - tests fork handling and chain reorganization.
    
    This test:
    1. Creates a main chain
    2. Creates a competing fork
    3. Measures reorganization time
    """
    
    def run(self) -> TestResult:
        """Run the fork test"""
        start_time = time.time()
        start_time_str = datetime.now().isoformat()
        
        nodes = self.node_manager.nodes
        errors: List[str] = []
        
        if not nodes:
            return TestResult(
                test_type="fork",
                success=False,
                start_time=start_time_str,
                end_time=datetime.now().isoformat(),
                duration_seconds=0,
                errors=["No nodes available"],
            )
        
        fork_length = self.config.custom_params.get("fork_length", 1000)
        
        logger.info(f"Starting fork test: {fork_length} blocks")
        
        # Initialize times to ensure they are always bound
        main_time = 0.0
        fork_time = 0.0
        switch_time = 0.0
        try:
            node = nodes[0]
            client = self.node_manager.get_rpc_client(node)
            
            # Get genesis
            genesis = client.get_best_block_hash()
            
            # Generate main chain
            logger.info(f"Generating main chain of {fork_length} blocks...")
            main_start = time.time()
            
            parent = genesis
            for i in range(fork_length):
                if self._stopped:
                    break
                    
                parent = client.generate_block_with_parent(parent, [])
                
                if (i + 1) % 100 == 0:
                    logger.info(f"[PROGRESS] Main chain: {i + 1}/{fork_length}")
            
            main_end = parent
            main_time = time.time() - main_start
            logger.info(f"Main chain generated in {main_time:.2f}s")
            
            # Generate fork (longer by 1)
            logger.info(f"Generating fork of {fork_length + 1} blocks...")
            fork_start = time.time()
            
            parent = genesis
            for i in range(fork_length + 1):
                if self._stopped:
                    break
                    
                parent = client.generate_block_with_parent(parent, [])
                
                if (i + 1) % 100 == 0:
                    logger.info(f"[PROGRESS] Fork: {i + 1}/{fork_length + 1}")
            
            fork_time = time.time() - fork_start
            logger.info(f"Fork generated in {fork_time:.2f}s")
            
            # Switch back to main chain
            logger.info("Switching back to main chain...")
            switch_start = time.time()
            
            parent = main_end
            for i in range(2):
                parent = client.generate_block_with_parent(parent, [])
            
            switch_time = time.time() - switch_start
            logger.info(f"Switch completed in {switch_time:.2f}s")
            
        except Exception as e:
            errors.append(f"Fork test error: {e}")
            logger.error(f"Fork test error: {e}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        return TestResult(
            test_type="fork",
            success=len(errors) == 0,
            start_time=start_time_str,
            end_time=datetime.now().isoformat(),
            duration_seconds=duration,
            blocks_generated=fork_length * 2 + 3,
            node_metrics=self.node_manager.collect_metrics(),
            errors=errors,
            extra={
                "main_chain_time": main_time if 'main_time' in dir() else 0,
                "fork_time": fork_time if 'fork_time' in dir() else 0,
                "switch_time": switch_time if 'switch_time' in dir() else 0,
            },
        )


class TestController:
    """
    Controls test execution on the Conflux network.
    
    Supports different test types and manages test lifecycle.
    """
    
    def __init__(
        self,
        config: DeploymentConfig,
        node_manager: NodeManager,
    ):
        """
        Initialize the test controller.
        
        Args:
            config: Deployment configuration
            node_manager: Node manager instance
        """
        self.config = config
        self.node_manager = node_manager
        self._current_test: Optional[BaseTest] = None
    
    def run_test(self, test_type: Optional[str] = None) -> TestResult:
        """
        Run a test.
        
        Args:
            test_type: Test type (defaults to config.test.test_type)
            
        Returns:
            TestResult
        """
        test_type = test_type or self.config.test.test_type
        test_config = self.config.test
        
        logger.info(f"Running {test_type} test")
        
        # Create test instance
        if test_type == "stress":
            self._current_test = StressTest(self.node_manager, test_config)
        elif test_type == "latency":
            self._current_test = LatencyTest(self.node_manager, test_config)
        elif test_type == "fork":
            self._current_test = ForkTest(self.node_manager, test_config)
        else:
            logger.warning(f"Unknown test type: {test_type}, running stress test")
            self._current_test = StressTest(self.node_manager, test_config)
        
        # Run the test
        try:
            result = self._current_test.run()
            logger.info(f"Test completed: success={result.success}")
            return result
        finally:
            self._current_test = None

    def run_stress_test(self, duration_seconds: int = 300) -> TestResult:
        """Compatibility wrapper for stress tests.

        Note: the current StressTest implementation is block-count based;
        duration_seconds is accepted for API compatibility.
        """
        _ = duration_seconds
        return self.run_test("stress")

    def run_latency_test(self, sample_count: int = 100) -> TestResult:
        """Compatibility wrapper for latency tests.

        sample_count is accepted for API compatibility.
        """
        _ = sample_count
        return self.run_test("latency")

    def run_fork_test(self, target_depth: int = 10) -> TestResult:
        """Compatibility wrapper for fork tests.

        target_depth is accepted for API compatibility.
        """
        _ = target_depth
        return self.run_test("fork")
    
    def stop_test(self) -> None:
        """Stop the current test"""
        if self._current_test:
            logger.info("Stopping test...")
            self._current_test.stop()
    
    def run_rpc_benchmark(self) -> Dict[str, Any]:
        """
        Run RPC benchmark to test node responsiveness.
        
        Returns:
            Benchmark results
        """
        nodes = self.node_manager.nodes
        results: Dict[str, Any] = {}
        
        for node in nodes:
            client = self.node_manager.get_rpc_client(node)
            
            # Measure get_status latency
            latencies = []
            for _ in range(10):
                start = time.time()
                try:
                    client.get_status()
                    latencies.append((time.time() - start) * 1000)
                except Exception:
                    pass
            
            results[node.node_id] = {
                "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
                "max_latency_ms": max(latencies) if latencies else 0,
                "min_latency_ms": min(latencies) if latencies else 0,
                "success_rate": len(latencies) / 10,
            }
        
        return results
