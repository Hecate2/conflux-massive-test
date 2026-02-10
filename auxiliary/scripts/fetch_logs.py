#!/usr/bin/env python3
"""Download logs from all hosts recorded in hosts.json into a target directory.

Example:
  .venv/bin/python auxiliary/scripts/fetch_logs.py --target logs/20260203153921
"""
import argparse
from pathlib import Path
import json
from loguru import logger
import subprocess
from concurrent.futures import ThreadPoolExecutor


def load_hosts(path: Path):
    data = json.load(open(path, 'r'))
    # Return raw dicts with expected keys: ip and nodes_per_host (plus optional ssh_user)
    return [item for item in data]


def scp_download_cli(remote_path: str, local_path: str, ip_address: str, *, user: str = 'root', key_path: str | None = None, max_retries: int = 3):
    key_args = []
    if key_path:
        key_args = ['-i', key_path]
    scp_cmd = [
        'scp',
        '-r',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        *key_args,
        f"{user}@{ip_address}:{remote_path}",
        local_path,
    ]
    for attempt in range(max_retries):
        try:
            subprocess.run(scp_cmd, check=True, capture_output=True, text=True, timeout=300)
            return
        except subprocess.CalledProcessError as e:
            if attempt == max_retries - 1:
                raise
        except subprocess.TimeoutExpired:
            if attempt == max_retries - 1:
                raise


def rsync_download_cli(remote_path: str, local_path: str, ip_address: str, *, user: str = 'root', key_path: str | None = None, compress_level: int = 12, max_retries: int = 3):
    key_opt = f" -i {key_path}" if key_path else ''
    ssh_cmd = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null{key_opt}"
    rsync_cmd = [
        'rsync',
        '-az',
        f'--compress-level={compress_level}',
        '--compress-choice=zstd',
        '--partial',
        '--stats',
        '-e', ssh_cmd,
        f'{user}@{ip_address}:{remote_path}',
        local_path,
    ]

    for attempt in range(max_retries):
        try:
            subprocess.run(rsync_cmd, check=True, capture_output=True, text=True, timeout=120)
            return
        except subprocess.CalledProcessError as e:
            # try scp as fallback on last attempt
            if attempt == max_retries - 1:
                scp_download_cli(remote_path, local_path, ip_address, user=user, key_path=key_path, max_retries=2)
                return
        except subprocess.TimeoutExpired:
            if attempt == max_retries - 1:
                scp_download_cli(remote_path, local_path, ip_address, user=user, key_path=key_path, max_retries=2)
                return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts', default='hosts.json')
    parser.add_argument('--target', required=True)
    parser.add_argument('--user', default='root')
    args = parser.parse_args()

    hosts = load_hosts(Path(args.hosts))
    target_base = Path(args.target)
    target_base.mkdir(parents=True, exist_ok=True)

    def _log_base(ssh_user: str) -> str:
        return "/root" if ssh_user == "root" else f"/home/{ssh_user}"

    def _download_with_fallback(ip: str, ssh_user: str, key_path: str | None, remote_archive: str, remote_dir: str, local: Path, i_host: int, idx: int) -> None:
        logger.info(f"{i_host}: Downloading archive from {ip}:{remote_archive} -> {local}")
        try:
            rsync_download_cli(remote_archive, str(local), ip, user=ssh_user, key_path=key_path)
            return
        except Exception as e:
            logger.warning(f"Archive download failed for {ip} (node {idx}): {e}")

        logger.info(f"{i_host}: Falling back to legacy logs from {ip}:{remote_dir} -> {local}")
        try:
            rsync_download_cli(remote_dir, str(local), ip, user=ssh_user, key_path=key_path)
        except Exception as e:
            logger.warning(f"Legacy log download failed for {ip} (node {idx}): {e}")

    def _download_archive(i_host: int, host: dict, idx: int) -> None:
        ip = host.get('ip')
        ssh_user = host.get('ssh_user') or args.user
        base_dir = _log_base(ssh_user)
        remote = f"{base_dir}/output{idx}.7z"
        remote_dir = f"{base_dir}/output{idx}"
        local = target_base / f"{ip}-{idx}"
        local.mkdir(parents=True, exist_ok=True)
        # prefer using host-specific key if available
        key_path = host.get('ssh_key_path') if isinstance(host, dict) else None
        _download_with_fallback(ip, ssh_user, key_path, remote, remote_dir, local, i_host, idx)

    download_tasks = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        for i_host, host in enumerate(hosts):
            ip = host.get('ip')
            nodes_per_host = int(host.get('nodes_per_host', 1))
            if not ip:
                logger.warning(f"Host {i_host} missing ip, skipping")
                continue
            for idx in range(nodes_per_host):
                download_tasks.append(executor.submit(_download_archive, i_host, host, idx))
        for task in download_tasks:
            task.result()

    # Retry missing files (blocks.log absent or empty)
    def _retry_missing(i_host: int, host: dict, idx: int) -> None:
        ip = host.get('ip')
        ssh_user = host.get('ssh_user') or args.user
        key_path = host.get('ssh_key_path')
        local = target_base / f"{ip}-{idx}"
        archive_file = local / f"output{idx}.7z"
        if not archive_file.exists() or archive_file.stat().st_size == 0:
            base_dir = _log_base(ssh_user)
            remote = f"{base_dir}/output{idx}.7z"
            remote_dir = f"{base_dir}/output{idx}"
            logger.info(f"Retrying download from {ip}:{remote} -> {local}")
            _download_with_fallback(ip, ssh_user, key_path, remote, remote_dir, local, i_host, idx)

    retry_tasks = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        for i, host in enumerate(hosts):
            ip = host.get('ip')
            nodes_per_host = int(host.get('nodes_per_host', 1))
            if not ip:
                logger.warning(f"Host {i} missing ip, skipping retry")
                continue
            for idx in range(nodes_per_host):
                retry_tasks.append(executor.submit(_retry_missing, i, host, idx))
        for task in retry_tasks:
            task.result()

    logger.success("Download finished")

if __name__ == '__main__':
    main()
