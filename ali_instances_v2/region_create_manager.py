from concurrent.futures import ThreadPoolExecutor
import copy
import json
import queue
import socket
import time
import threading
from typing import Dict, List, Set, Tuple
from queue import Queue


from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from loguru import logger

from utils.wait_until import WaitUntilTimeoutError, wait_until

from .types import Instance, InstanceType

SSH_CHECK_POOL = ThreadPoolExecutor(max_workers=2000)


class RegionCreateManager:
    region_id: str
    target_nodes: int
    request_nodes: int
    ready_instances: List[Tuple[Instance, str]]
    pending_instances: Dict[str, Instance]

    _event: threading.Event
    _lock: threading.RLock
    _running_queue: Queue[Dict[str, str]]

    def __init__(self, region_id: str, target_nodes: int, additional_nodes: int = 0):
        self.region_id = region_id
        self.target_nodes = target_nodes
        self.request_nodes = target_nodes + additional_nodes
        self.ready_instances = []
        self.pending_instances = dict()
        self._event = threading.Event()
        self._lock = threading.RLock()
        self._running_queue = Queue(maxsize=10000)

    def submit_pending_instances(self, ids: List[str], type: InstanceType):
        self.pending_instances.update(
            {id: Instance(instance_id=id, type=type) for id in ids})

    @property
    def ready_nodes(self):
        with self._lock:
            return sum([instance.type.nodes for instance, _ in self.ready_instances])

    def copy_ready_instances(self):
        with self._lock:
            return copy.copy(self.ready_instances)

    @property
    def pending_nodes(self):
        with self._lock:
            return sum([instance.type.nodes for instance in self.pending_instances.values()])

    def get_rest_nodes(self, *, wait_for_pendings=False):
        while True:
            with self._lock:
                ready_nodes = self.ready_nodes
                pending_nodes = self.pending_nodes

                # 如果 ready 满足目标，任务已完成
                if ready_nodes >= self.target_nodes:
                    return 0

                # 如果 ready 和 pending 不足目标，直接返回差值
                if ready_nodes + pending_nodes < self.request_nodes and (not wait_for_pendings or pending_nodes == 0):
                    return self.request_nodes - ready_nodes - pending_nodes

            # 剩下的情况里，ready 不满足，但 ready + pending 满足，或者 wait_for_pendings 是 true，需要等待 pending 的结果
            normal = self._event.wait(timeout=180)
            if not normal:
                raise Exception(
                    f"Region {self.region_id} wait for event timeout")

    def describe_instances_loop(self, client: EcsClient, check_interval: float = 3.0):
        processed_instances: Set[str] = set()

        while True:
            # 获取当前 pending instance
            with self._lock:
                to_check_instances = set(
                    self.pending_instances) - processed_instances

            running_instances, pending_instances = _describe_instance(
                client, self.region_id, instance_ids=list(to_check_instances))

            if len(pending_instances) > 0:
                logger.debug(
                    f"Instances {pending_instances} pending in region {self.region_id}")

            # 将 running instance 转入下一阶段
            if len(running_instances) > 0:
                logger.success(
                    f"Instances {running_instances} running in region {self.region_id}")
                processed_instances |= set(running_instances)
                self._running_queue.put(running_instances)

            # 将 lost instance 删除
            lost_instances = to_check_instances - \
                set(running_instances) - pending_instances

            with self._lock:
                if len(lost_instances) > 0:
                    logger.info(
                        f"Instances {lost_instances} lost or stopped in region {self.region_id}")
                    for instance_id in lost_instances:
                        del self.pending_instances[instance_id]
                    self._event.set()

                if self.ready_nodes >= self.target_nodes:
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread describe_instances loop quit")
                    return

            time.sleep(check_interval)

    def wait_for_ssh_loop(self):
        future_set = dict()
        while True:
            # 从队列获取任务并提交
            try:
                running_instances = self._running_queue.get_nowait()
                for instance_id, ip in running_instances.items():
                    check_future = SSH_CHECK_POOL.submit(
                        _wait_for_ssh_port_ready, ip)
                    future_set[instance_id] = (ip, check_future)
            except queue.Empty:
                pass

            # 查看线程池结果
            to_clear_instance_ids = set()
            for instance_id, (ip, future) in future_set.items():
                if not future.done():
                    continue

                to_clear_instance_ids.add(instance_id)
                is_success = future.result()

                if is_success:
                    logger.info(
                        f"Region {self.region_id} Instance {instance_id} IP {ip} connect success")

                    with self._lock:
                        instance = self.pending_instances[instance_id]
                        del self.pending_instances[instance_id]
                        self.ready_instances.append((instance, ip))
                else:
                    logger.info(
                        f"Region {self.region_id} Instance {instance_id} IP {ip} connect fail (timeout)")
                    with self._lock:
                        del self.pending_instances[instance_id]

            for instance_id in to_clear_instance_ids:
                del future_set[instance_id]

            if to_clear_instance_ids:
                self._event.set()

            with self._lock:
                if self.ready_nodes >= self.target_nodes:
                    logger.info(
                        f"Region {self.region_id} reach target nodes, thread wait_for_ssh_loop quit")
                    return

            time.sleep(1)


def _describe_instance(client: EcsClient, region_id: str, instance_ids: List[str]):
    running_instances = dict()
    pending_instances = set()

    for i in range(0, len(instance_ids), 100):
        query_chunk = instance_ids[i: i+100]
        
        rep = client.describe_instances(ecs_models.DescribeInstancesRequest(
            region_id=region_id, page_size=100, instance_ids=json.dumps(query_chunk)))
        instance_status = rep.body.instances.instance

        running_instances.update(
            {i.instance_id: i.public_ip_address.ip_address[0] for i in instance_status if i.status in ["Running"]})
        
        # 阿里云启动阶段也可能读到 instance 是 stopped 的状态
        pending_instances.update({i.instance_id for i in instance_status if i.status in [
                                 "Starting", "Pending", "Stopped"]})
        time.sleep(0.5)
    return running_instances, pending_instances


def _check_port(ip: str, timeout: int = 5):
    """
    处理单个IP端口检查任务

    Args:
        ip: IP地址
        port: 端口号
        attempt: 当前尝试次数
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        result = sock.connect_ex((ip, 22))
        return result == 0
    except (socket.timeout, socket.error):
        return False
    finally:
        sock.close()


def _wait_for_ssh_port_ready(ip: str):
    try:
        wait_until(lambda: _check_port(ip), timeout=180)
        return True
    except WaitUntilTimeoutError:
        logger.warning(f"Cannot connect to IP {ip}")
        return False
