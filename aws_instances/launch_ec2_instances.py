
# Original file: launch-on-demand.sh

import argparse
import datetime
import json
import os
from pathlib import Path

from dotenv import load_dotenv

import traceback
from typing import List, Optional, Literal, Iterable
from dataclasses import dataclass
import boto3
import subprocess
import time
import utils.shell_cmds as shell_cmds
from concurrent.futures import ThreadPoolExecutor
from aws_instances.get_instance_ips import get_ec2_instance_ips
from aws_instances.aws_config import AwsRegionPlan, load_region_plans, instances_needed
from loguru import logger
from utils.counter import AtomicCounter
from ali_instances.host_spec import HostSpec


SSH_CONNECT_CHECK_POOL = ThreadPoolExecutor(max_workers=400)

@dataclass
class LaunchConfig:
    """EC2 实例启动配置"""
    instance_count: int
    keypair: str
    role: str
    image_id: str = "ami-09614fb0a0d1c1c5e"
    instance_type: str = "m5.4xlarge"
    use_public_ip: bool = False
    aws_region: str = "us-west-2"
    security_group_id: Optional[str] = "sg-050516f8dcd5470c6"
    subnet_id: Optional[str] = None
    volume_size: int = 250
    ssh_user: str = "ubuntu"
    nodes_per_host: int = 1
    access_key_id: Optional[str] = None
    access_key_secret: Optional[str] = None
    user_tag: Optional[str] = None

@dataclass
class Instances:
    """EC2 实例启动结果"""

    config: LaunchConfig
    instance_ids: List[str]
    status: Literal["created"] | Literal["running"] | Literal["stopping"]
    ip_addresses: Optional[List[str]] = None

    def _ec2_client(self):
        if self.config.access_key_id and self.config.access_key_secret:
            session = boto3.Session(
                aws_access_key_id=self.config.access_key_id,
                aws_secret_access_key=self.config.access_key_secret,
                region_name=self.config.aws_region,
            )
        else:
            session = boto3.Session(region_name=self.config.aws_region)
        return session.client("ec2", region_name=self.config.aws_region)

    @classmethod
    def launch(cls, config: LaunchConfig) -> 'Instances':
        """
        创建一组 EC2 实例
        
        参数:
            config: 实例启动配置
        
        返回:
            Instances: 追踪 AWS 实例组的对象
        """
        # 创建 EC2 客户端
        if config.access_key_id and config.access_key_secret:
            session = boto3.Session(
                aws_access_key_id=config.access_key_id,
                aws_secret_access_key=config.access_key_secret,
                region_name=config.aws_region,
            )
        else:
            session = boto3.Session(region_name=config.aws_region)
        ec2_client = session.client('ec2', region_name=config.aws_region)
        
        # 启动实例并确保实例已经运行
        logger.info(f"启动 {config.instance_count} 个 EC2 实例...")
        tags = [
            {'Key': 'role', 'Value': config.role},
            {'Key': 'Name', 'Value': f"{config.instance_type}-{config.image_id}"},
            {'Key': 'nodes_per_host', 'Value': str(config.nodes_per_host)},
        ]
        if config.user_tag:
            tags.append({'Key': 'user', 'Value': config.user_tag})

        params = {
            'ImageId': config.image_id,
            'MinCount': config.instance_count,
            'MaxCount': config.instance_count,
            'KeyName': config.keypair,
            'InstanceType': config.instance_type,
            'BlockDeviceMappings': [
                {
                    'DeviceName': '/dev/sda1',
                    'Ebs': {
                        'VolumeSize': config.volume_size,
                        'VolumeType': 'gp3',
                        'Iops': 3000,
                        # 4. gp3 单卷吞吐量最大值 (MiB/s)
                        'Throughput': 300,
                        # 5. 随实例终止删除，避免残留扣费
                        'DeleteOnTermination': True
                    },
                }
            ],
            'TagSpecifications': [
                {
                    'ResourceType': 'instance',
                    'Tags': tags
                }
            ]
        }

        if config.security_group_id:
            params['SecurityGroupIds'] = [config.security_group_id]
        if config.subnet_id:
            params['SubnetId'] = config.subnet_id

        response = ec2_client.run_instances(**params)
        
        # 提取实例 ID
        instance_ids =  [instance['InstanceId'] for instance in response['Instances']]
        
        # 验证创建的实例数量
        if len(instance_ids) != config.instance_count:
            raise ValueError(
                f"实例创建数量不足: 需要 {config.instance_count}, 实际创建 {len(instance_ids)}"
            )
        
        return Instances(instance_ids=instance_ids, status="created", config=config)
    

    def wait_for_all_running(self, check_interval = 3):
        ec2_client = self._ec2_client()

        if self.status == "running":
            return
        elif self.status != "created":
            raise Exception(f"Incorrect status {self.status} while waiting for instance launch")
        
        instance_ids = list(self.instance_ids)
        while True:
            response = ec2_client.describe_instances(InstanceIds=instance_ids)
            running_count = 0
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    if instance.get('State', {}).get('Name') == 'running':
                        running_count += 1

            logger.info(f"{running_count} 个实例正在运行...")

            if running_count >= self.config.instance_count:
                break

            time.sleep(check_interval)

    
    def check_ssh_connection(
        self,
        ssh_user: Optional[str] = None,
        check_interval: int = 3,
        timeout: int = 5,
        max_retries: int = 20,
    ):
        """
        等待实例可以通过 SSH 连接
        
        参数:
            instance_ids: 实例 ID 列表
            ip_type: IP 类型
            aws_region: AWS 区域
            ssh_user: SSH 用户名
            check_interval: 检查间隔（秒）
        
        返回:
            可连接的 IP 地址列表
        """
        if self.ip_addresses is not None:
            logger.warning("已经进行过 SSH 链接检查")
            return

        logger.debug("等待实例可以通过 SSH 连接...")

        ip_type = "public" if self.config.use_public_ip else "private"

        if ssh_user is None:
            ssh_user = self.config.ssh_user

        ips = get_ec2_instance_ips(
            instance_ids=self.instance_ids,
            ip_type=ip_type,
            aws_region=self.config.aws_region
        )

        logger.debug("取得所有实例的 ip")

        def test_single_connection(ip: str) -> bool:
            """测试单个 SSH 连接，返回是否成功"""
            result = subprocess.run(
                [
                    "ssh",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", f"ConnectTimeout={timeout}",
                    "-o", "BatchMode=yes",
                    f"{ssh_user}@{ip}",
                    "exit"
                ],
                check=False,
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        
        def try_connect_single_ip(ip: str, counter: AtomicCounter) -> bool:
            retries = 0
            while not test_single_connection(ip):
                retries += 1
                logger.debug(f"实例 {ip} 连接测试失败，重试 {retries} / {max_retries}")
                if retries >= max_retries:
                    return False
                time.sleep(check_interval)

            counter_idx = counter.increment()
            logger.info(f"实例 {ip} 连接测试成功 ({counter_idx}/{len(ips)})")
            return True
        
        counter = AtomicCounter()

        # 并行执行 SSH 测试
        success_count = sum(SSH_CONNECT_CHECK_POOL.map(lambda ip: try_connect_single_ip(ip, counter), ips))
        
        if success_count < len(ips):
            raise Exception(f"部分实例无法链接，实例组 {self}")

        logger.info(f"完成 {len(ips)} 个实例的 ip 检查")
        
        self.ip_addresses = ips

    def remote_execute(self, script_path: str, args: List[str] = None):
        if args is None:
            args = list()

        if self.ip_addresses is None:
            logger.warning("尚未进行过 SSH 链接检查")
            return
        
        setup_script_name = os.path.basename(script_path)

        def execute_one(ip_address: str, counter: AtomicCounter):
            try:
                shell_cmds.scp(script_path, ip_address, self.config.ssh_user)
                
                shell_cmds.ssh(ip_address, self.config.ssh_user, command=[f'./{setup_script_name}', *args])

                counter_idx = counter.increment()
                logger.info(f"{ip_address} 执行脚本成功 ({counter_idx}/{len(self.ip_addresses)})")

                return 0
            except Exception as e:
                logger.warning(f"{ip_address} 执行脚本失败: {e}")
                return 1
            
        counter = AtomicCounter()
        failure_count = sum(SSH_CONNECT_CHECK_POOL.map(lambda ip: execute_one(ip, counter), self.ip_addresses))
        
        if failure_count > 0:
            raise Exception(f"部分实例执行失败，实例组 {self}")

        logger.info(f"完成 {len(self.ip_addresses)} 个实例的脚本执行")

    def terminate(self):
        """终止所有实例"""
        if self.status == "stopping":
            logger.warning("实例已经在终止中")
            return
        
        ec2_client = self._ec2_client()
        
        logger.info(f"终止 {len(self.instance_ids)} 个实例...")
        ec2_client.terminate_instances(InstanceIds=self.instance_ids)
        
        self.status = "stopping"
        logger.info(f"发送终止请求: {self.instance_ids}")

def launch(config: LaunchConfig, dump_file: Optional[str]):
    # 启动实例
    instances = Instances.launch(config)
    
    try:
        instances.wait_for_all_running()
        instances.check_ssh_connection()

        instances.remote_execute("./setup_image.sh")
        
        # 使用结果
        logger.success(f"完成 {config.instance_count} 个实例创建")
        logger.info(f"实例 ID: {instances.instance_ids}")
        logger.info(f"IP 地址: {instances.ip_addresses}")

        root = Path(__file__).resolve().parents[1]
        timestamp = generate_timestamp()
        log_dir = root / "logs" / timestamp
        hosts = [
            HostSpec(
                ip=ip,
                nodes_per_host=config.nodes_per_host,
                ssh_user=config.ssh_user,
                ssh_key_path=None,
                provider="aws",
                region=config.aws_region,
                instance_id=instance_id,
            )
            for instance_id, ip in zip(instances.instance_ids, instances.ip_addresses or [])
        ]
        data = write_inventory(hosts, timestamp, log_dir, root)

        if dump_file:
            dump_path = Path(dump_file).resolve()
            dump_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
            logger.info(f"实例信息已写入 inventory json: {dump_path}")
    except Exception:
        traceback.print_exc() 
        logger.warning(f"遇到错误，销毁所有实例")
        instances.terminate()
        logger.success(f"完成实例销毁")


def generate_timestamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def serialize_host(host: HostSpec) -> dict:
    return {
        "ip": host.ip,
        "nodes_per_host": host.nodes_per_host,
        "ssh_user": host.ssh_user,
        "ssh_key_path": host.ssh_key_path,
        "provider": host.provider,
        "region": host.region,
        "instance_id": host.instance_id,
    }


def write_inventory(hosts: Iterable[HostSpec], timestamp: str, log_dir: Path, root: Path) -> dict:
    data = {
        "timestamp": timestamp,
        "log_dir": str(log_dir.as_posix()),
        "hosts": [serialize_host(h) for h in hosts],
    }
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "aws_servers.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))
    (root / "aws_servers.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return data


def distribute_instance_count(instance_count: int, subnet_ids: List[str]) -> List[tuple[Optional[str], int]]:
    if not subnet_ids:
        return [(None, instance_count)]
    buckets = [0] * len(subnet_ids)
    for i in range(instance_count):
        buckets[i % len(subnet_ids)] += 1
    return [(subnet_ids[i], count) for i, count in enumerate(buckets) if count > 0]


def launch_region_plan(
    plan: AwsRegionPlan,
    keypair: str,
    role: str,
    use_public_ip: bool,
    setup_script: Optional[str],
) -> tuple[List[Instances], List[HostSpec]]:
    created_instances: List[Instances] = []
    hosts: List[HostSpec] = []
    remaining_nodes = plan.node_count

    for type_spec in plan.type_specs:
        if remaining_nodes <= 0:
            break
        instance_count = instances_needed(remaining_nodes, type_spec.nodes_per_host)
        allocations = distribute_instance_count(instance_count, plan.subnet_ids)

        try:
            for subnet_id, count in allocations:
                config = LaunchConfig(
                    instance_count=count,
                    keypair=keypair,
                    role=role,
                    image_id=plan.image_id,
                    instance_type=type_spec.name,
                    use_public_ip=use_public_ip,
                    aws_region=plan.region_name,
                    security_group_id=plan.security_group_id or "",
                    subnet_id=subnet_id,
                    nodes_per_host=type_spec.nodes_per_host,
                    access_key_id=plan.access_key_id,
                    access_key_secret=plan.access_key_secret,
                    user_tag=plan.user_tag,
                )
                instances = Instances.launch(config)
                instances.wait_for_all_running()
                instances.check_ssh_connection()
                if setup_script:
                    instances.remote_execute(setup_script)
                created_instances.append(instances)

                for instance_id, ip in zip(instances.instance_ids, instances.ip_addresses or []):
                    hosts.append(
                        HostSpec(
                            ip=ip,
                            nodes_per_host=type_spec.nodes_per_host,
                            ssh_user=config.ssh_user,
                            ssh_key_path=None,
                            provider="aws",
                            region=plan.region_name,
                            instance_id=instance_id,
                        )
                    )
            remaining_nodes = max(0, remaining_nodes - instance_count * type_spec.nodes_per_host)
        except Exception as exc:
            logger.warning(f"Region {plan.region_name} type {type_spec.name} launch failed: {exc}")
            continue

    if remaining_nodes > 0:
        raise RuntimeError(f"Region {plan.region_name} still missing {remaining_nodes} nodes")

    return created_instances, hosts


def launch_from_config(
    config_path: Path,
    hardware_path: Path,
    keypair: str,
    role: str,
    use_public_ip: bool,
    dump_file: str,
    setup_script: Optional[str],
) -> None:
    root = Path(__file__).resolve().parents[1]
    plans = load_region_plans(config_path, hardware_path)
    if not plans:
        raise RuntimeError("no AWS regions with count > 0 in config")

    all_instances: List[Instances] = []
    all_hosts: List[HostSpec] = []

    try:
        for plan in plans:
            instances, hosts = launch_region_plan(plan, keypair, role, use_public_ip, setup_script)
            all_instances.extend(instances)
            all_hosts.extend(hosts)

        timestamp = generate_timestamp()
        log_dir = root / "logs" / timestamp
        data = write_inventory(all_hosts, timestamp, log_dir, root)
        if dump_file:
            dump_path = Path(dump_file).resolve()
            dump_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        logger.success(f"完成 AWS 实例创建，inventory 写入 {log_dir}/aws_servers.json")
    except Exception:
        traceback.print_exc()
        logger.warning("遇到错误，销毁所有实例")
        for instances in all_instances:
            instances.terminate()
        logger.success("完成实例销毁")


def terminate_from_inventory(inventory_path: Path) -> None:
    payload = json.loads(inventory_path.read_text())
    hosts = payload.get("hosts", []) if isinstance(payload, dict) else []
    if not isinstance(hosts, list):
        raise ValueError("invalid aws_servers.json format")

    instances_by_region: dict[str, list[str]] = {}
    for item in hosts:
        if not isinstance(item, dict):
            continue
        instance_id = item.get("instance_id")
        region = item.get("region")
        if not instance_id or not region:
            continue
        instances_by_region.setdefault(region, []).append(instance_id)

    if not instances_by_region:
        raise RuntimeError("no instance ids found in inventory")

    for region, instance_ids in instances_by_region.items():
        ec2_client = boto3.client("ec2", region_name=region)
        logger.info(f"终止 {region} 区域 {len(instance_ids)} 个实例...")
        ec2_client.terminate_instances(InstanceIds=instance_ids)
    logger.success("完成实例销毁")

def arg_parser():
    parser = argparse.ArgumentParser(description="Manage EC2 instances")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # create subcommand
    create = subparsers.add_parser("create", help="Create instances", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    create.add_argument("-f", "--dump-file", default="aws_servers.json", help="Path to the instance inventory json")
    create.add_argument("-n", "--instance-count", default=3, type=int, help="Number of instances")
    create.add_argument("-t", "--instance-type", default="m6i.2xlarge", help="Instance Type")
    create.add_argument("-w", "--instance-weight", default=1, type=int, help="Instance Weight")
    create.add_argument("--config", default=None, help="Path to instance-region.json (enable multi-region mode)")
    create.add_argument("--hardware", default="config/hardware.json", help="Path to hardware.json")
    create.add_argument("--role", default="massive-test", help="Tag value for role")
    create.add_argument("--use-public-ip", action="store_true", help="Use public IP addresses")
    create.add_argument("--setup-script", default="./setup_image.sh", help="Script to run on instances (empty to skip)")
    
    # destroy subcommand
    destroy = subparsers.add_parser("destroy", help="Destroy instances", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    destroy.add_argument("-f", "--dump-file", default="aws_servers.json", help="Path to the instance inventory json")

    return parser


# ========== 使用示例 ==========

if __name__ == "__main__":
    load_dotenv()

    keypair = os.getenv("CONFLUX_MASSIVE_TEST_AWS_KEY_PAIR", "chenxing-st")

    parser = arg_parser()
    args = parser.parse_args()
    
    if args.command == "create":
        if args.config:
            root = Path(__file__).resolve().parents[1]
            config_path = (root / args.config).resolve()
            hardware_path = (root / args.hardware).resolve()
            setup_script = args.setup_script or None
            launch_from_config(
                config_path=config_path,
                hardware_path=hardware_path,
                keypair=keypair,
                role=args.role,
                use_public_ip=args.use_public_ip,
                dump_file=args.dump_file,
                setup_script=setup_script,
            )
        else:
            # 配置启动参数
            config = LaunchConfig(
                instance_count=args.instance_count,
                keypair=keypair,
                role=args.role,
                image_id="ami-088f930e18817a524",
                instance_type=args.instance_type,
                use_public_ip=args.use_public_ip,
                aws_region="us-west-2"
            )
            launch(config, args.dump_file)
    elif args.command == "destroy":
        inventory_path = Path(args.dump_file).resolve()
        terminate_from_inventory(inventory_path)
