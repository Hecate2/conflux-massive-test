import asyncio
import time
from pathlib import Path
from typing import List

import asyncssh

from loguru import logger


def _run_async(coro):
    return asyncio.run(coro)


async def _wait_ssh_ready(ip_address: str, user: str, key_path: str | None, timeout: int, interval: int):
    if not key_path:
        return
    from ali_instances.instance_prep import wait_ssh

    await wait_ssh(ip_address, user, key_path, timeout, interval)


def _connect_kwargs(user: str, key_path: str | None) -> dict:
    kwargs = {"username": user, "known_hosts": None}
    if key_path:
        kwargs["client_keys"] = [key_path]
    return kwargs


async def _retry_async(action, *, timeout: int, retry_delay: int):
    deadline = time.time() + timeout
    attempt = 0
    while True:
        try:
            return await action()
        except Exception as exc:
            attempt += 1
            if time.time() + retry_delay >= deadline:
                raise
            logger.debug(f"SSH retry {attempt}, retry in {retry_delay}s: {exc}")
            await asyncio.sleep(retry_delay)

def scp(
    script_path: str,
    ip_address: str,
    user: str = "ubuntu",
    remote_path: str = "~",
    *,
    key_path: str | None = None,
    retry_delay: int = 10,
    timeout: int = 60,
):
    async def _do():
        await _wait_ssh_ready(ip_address, user, key_path, timeout, 3)
        async with asyncssh.connect(ip_address, **_connect_kwargs(user, key_path)) as conn:
            await asyncssh.scp(script_path, (conn, remote_path))

    _run_async(_retry_async(_do, timeout=timeout, retry_delay=retry_delay))

def rsync_download(
    remote_path: str,
    local_path: str,
    ip_address: str,
    *,
    user: str = "ubuntu",
    key_path: str | None = None,
    compress_level: int = 12,
    retry_delay: int = 10,
    timeout: int = 120,
):
    async def _do():
        await _wait_ssh_ready(ip_address, user, key_path, timeout, 3)
        Path(local_path).mkdir(parents=True, exist_ok=True)
        async with asyncssh.connect(ip_address, **_connect_kwargs(user, key_path)) as conn:
            await asyncssh.scp((conn, remote_path), local_path, recurse=True)

    _run_async(_retry_async(_do, timeout=timeout, retry_delay=retry_delay))

def ssh(
    ip_address: str,
    user: str = "ubuntu",
    command: str | List[str] | None = None,
    *,
    key_path: str | None = None,
    retry_delay: int = 15,
    timeout: int = 60,
):
    if command is None:
        return
    
    if type(command) is str:
        command = [command]

    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
    ]
    if key_path:
        ssh_cmd.extend(["-i", key_path])
    ssh_cmd.extend(
        [
            f"{user}@{ip_address}",
            *command,
        ]
    )

    async def _do():
        await _wait_ssh_ready(ip_address, user, key_path, timeout, 3)
        async with asyncssh.connect(ip_address, **_connect_kwargs(user, key_path)) as conn:
            return await conn.run(" ".join(command), check=True)

    return _run_async(_retry_async(_do, timeout=timeout, retry_delay=retry_delay))