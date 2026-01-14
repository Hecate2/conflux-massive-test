"""Test Controller"""

import time
from typing import Dict, Any, List
from dataclasses import dataclass

from loguru import logger
from conflux_deployer.configs import ConfigManager
from conflux_deployer.node_management import NodeManager, NodeInfo


@dataclass
class TestResult:
    """Test result information"""
    test_type: str
    status: str
    start_time: str
    end_time: str
    duration: float
    metrics: Dict[str, Any]
    errors: List[str]


class TestController:
    """Test Controller for Conflux network tests"""
    
    def __init__(self, config_manager: ConfigManager, node_manager: NodeManager):
        """Initialize Test Controller"""
        self.config_manager = config_manager
        self.node_manager = node_manager
        self.test_results: Dict[str, TestResult] = {}
    
    def run_test(self, test_type: str, test_config: Dict[str, Any]) -> TestResult:
        """Run test on Conflux network"""
        logger.info(f"Starting {test_type} test with config: {test_config}")
        
        start_time = time.time()
        start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            if test_type == "tps":
                result = self._run_tps_test(test_config)
            elif test_type == "latency":
                result = self._run_latency_test(test_config)
            elif test_type == "stability":
                result = self._run_stability_test(test_config)
            elif test_type == "stress":
                result = self._run_stress_test(test_config)
            else:
                raise ValueError(f"Unsupported test type: {test_type}")
            
            status = "success"
            errors = []
        except Exception as e:
            logger.error(f"Test {test_type} failed: {e}")
            status = "failed"
            errors = [str(e)]
            result = {}
        
        end_time = time.time()
        end_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        
        test_result = TestResult(
            test_type=test_type,
            status=status,
            start_time=start_time_str,
            end_time=end_time_str,
            duration=duration,
            metrics=result,
            errors=errors
        )
        
        # Store test result
        test_id = f"test-{test_type}-{int(time.time())}"
        self.test_results[test_id] = test_result
        
        logger.info(f"Test {test_type} completed in {duration:.2f} seconds with status: {status}")
        return test_result
    
    def _run_tps_test(self, test_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run TPS (Transactions Per Second) test"""
        logger.info("Running TPS test")
        
        # Get test configuration
        duration = test_config.get("duration", 300)
        tx_rate = test_config.get("tx_rate", 1000)
        
        # Get all nodes
        nodes = self.node_manager.list_nodes(status="running")
        if not nodes:
            raise ValueError("No running nodes found for test")
        
        # Select test nodes
        test_nodes = nodes[:min(10, len(nodes))]  # Use up to 10 nodes for testing
        
        # Simulate TPS test
        logger.info(f"Running TPS test with {len(test_nodes)} nodes, duration: {duration}s, tx_rate: {tx_rate}/s")
        
        # TODO: Implement actual TPS test logic
        # For now, just simulate the process
        time.sleep(duration)
        
        # Generate mock results
        metrics = {
            "average_tps": tx_rate * 0.8,  # Simulate 80% of target rate
            "peak_tps": tx_rate * 1.1,     # Simulate peak rate
            "success_rate": 99.9,          # Simulate success rate
            "total_transactions": tx_rate * duration,
            "test_nodes": len(test_nodes)
        }
        
        logger.info(f"TPS test completed with average TPS: {metrics['average_tps']}")
        return metrics
    
    def _run_latency_test(self, test_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run latency test"""
        logger.info("Running latency test")
        
        # Get test configuration
        duration = test_config.get("duration", 300)
        tx_count = test_config.get("tx_count", 1000)
        
        # Get all nodes
        nodes = self.node_manager.list_nodes(status="running")
        if not nodes:
            raise ValueError("No running nodes found for test")
        
        # Simulate latency test
        logger.info(f"Running latency test with {len(nodes)} nodes, duration: {duration}s, tx_count: {tx_count}")
        
        # TODO: Implement actual latency test logic
        # For now, just simulate the process
        time.sleep(duration)
        
        # Generate mock results
        metrics = {
            "average_latency": 0.5,  # Simulate 500ms average latency
            "p95_latency": 1.2,     # Simulate 95th percentile latency
            "p99_latency": 2.0,     # Simulate 99th percentile latency
            "minimum_latency": 0.1,  # Simulate minimum latency
            "maximum_latency": 3.0,  # Simulate maximum latency
            "test_nodes": len(nodes)
        }
        
        logger.info(f"Latency test completed with average latency: {metrics['average_latency']}s")
        return metrics
    
    def _run_stability_test(self, test_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run stability test"""
        logger.info("Running stability test")
        
        # Get test configuration
        duration = test_config.get("duration", 3600)  # 1 hour
        
        # Get all nodes
        nodes = self.node_manager.list_nodes(status="running")
        if not nodes:
            raise ValueError("No running nodes found for test")
        
        # Simulate stability test
        logger.info(f"Running stability test with {len(nodes)} nodes, duration: {duration}s")
        
        # TODO: Implement actual stability test logic
        # For now, just simulate the process
        time.sleep(min(duration, 300))  # Simulate 5 minutes for testing
        
        # Generate mock results
        metrics = {
            "uptime": 100.0,           # Simulate 100% uptime
            "node_failures": 0,        # Simulate no node failures
            "network_partitions": 0,   # Simulate no network partitions
            "consensus_issues": 0,     # Simulate no consensus issues
            "test_nodes": len(nodes)
        }
        
        logger.info(f"Stability test completed with uptime: {metrics['uptime']}%")
        return metrics
    
    def _run_stress_test(self, test_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run stress test"""
        logger.info("Running stress test")
        
        # Get test configuration
        duration = test_config.get("duration", 600)  # 10 minutes
        max_tx_rate = test_config.get("max_tx_rate", 5000)
        
        # Get all nodes
        nodes = self.node_manager.list_nodes(status="running")
        if not nodes:
            raise ValueError("No running nodes found for test")
        
        # Simulate stress test
        logger.info(f"Running stress test with {len(nodes)} nodes, duration: {duration}s, max_tx_rate: {max_tx_rate}/s")
        
        # TODO: Implement actual stress test logic
        # For now, just simulate the process
        time.sleep(min(duration, 300))  # Simulate 5 minutes for testing
        
        # Generate mock results
        metrics = {
            "maximum_tps_achieved": max_tx_rate * 0.9,  # Simulate 90% of max rate
            "node_stability": 100.0,                   # Simulate 100% node stability
            "network_stability": 100.0,                # Simulate 100% network stability
            "test_nodes": len(nodes)
        }
        
        logger.info(f"Stress test completed with maximum TPS: {metrics['maximum_tps_achieved']}")
        return metrics
    
    def get_test_result(self, test_id: str) -> Dict[str, Any]:
        """Get test result by ID"""
        test_result = self.test_results.get(test_id)
        if not test_result:
            raise ValueError(f"Test result not found: {test_id}")
        
        return {
            "test_type": test_result.test_type,
            "status": test_result.status,
            "start_time": test_result.start_time,
            "end_time": test_result.end_time,
            "duration": test_result.duration,
            "metrics": test_result.metrics,
            "errors": test_result.errors
        }
    
    def list_test_results(self, test_type: str = None) -> List[Dict[str, Any]]:
        """List test results"""
        results = []
        for test_id, test_result in self.test_results.items():
            if test_type and test_result.test_type != test_type:
                continue
            results.append({
                "test_id": test_id,
                "test_type": test_result.test_type,
                "status": test_result.status,
                "start_time": test_result.start_time,
                "end_time": test_result.end_time,
                "duration": test_result.duration,
                "metrics": test_result.metrics
            })
        return results
    
    def wait_for_nodes_ready(self, timeout: int = 300):
        """Wait for all nodes to be ready for testing"""
        logger.info("Waiting for nodes to be ready for testing")
        return self.node_manager.wait_for_nodes_ready(timeout)
