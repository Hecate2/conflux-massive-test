import asyncio
import time
from pathlib import Path
from typing import Sequence

import asyncssh
from loguru import logger


def _connect_kwargs(user: str, key_path: str | None) -> dict:
    kwargs = {"username": user, "known_hosts": None}
    if key_path:
        kwargs["client_keys"] = [key_path]
    return kwargs


async def wait_ssh(host: str, user: str, key: str, timeout: int, interval: int = 3) -> None:
    """Wait until SSH is ready on a host."""
    kp = str(Path(key).expanduser())
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = await asyncssh.connect(host, username=user, client_keys=[kp], known_hosts=None)
            conn.close()
            await conn.wait_closed()
            return
        except Exception:
            await asyncio.sleep(interval)
    raise TimeoutError(f"SSH not ready for {host}")


async def _wait_ssh_ready(ip_address: str, user: str, key_path: str | None, timeout: int, interval: int) -> None:
    if not key_path:
        return
    await wait_ssh(ip_address, user, key_path, timeout, interval)


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


async def run_ssh(
    ip_address: str,
    user: str,
    command: str | Sequence[str],
    *,
    key_path: str | None = None,
    retry_delay: int = 10,
    timeout: int = 90,
    check: bool = True,
):
    if isinstance(command, (list, tuple)):
        command = " ".join(str(part) for part in command)

    async def _do():
        await _wait_ssh_ready(ip_address, user, key_path, timeout, 3)
        async with asyncssh.connect(ip_address, **_connect_kwargs(user, key_path)) as conn:
            return await conn.run(command, check=check)

    return await _retry_async(_do, timeout=timeout, retry_delay=retry_delay)


async def scp_upload(
    local_path: str,
    ip_address: str,
    user: str,
    remote_path: str,
    *,
    key_path: str | None = None,
    retry_delay: int = 10,
    timeout: int = 90,
):
    async def _do():
        await _wait_ssh_ready(ip_address, user, key_path, timeout, 3)
        async with asyncssh.connect(ip_address, **_connect_kwargs(user, key_path)) as conn:
            await asyncssh.scp(local_path, (conn, remote_path))

    await _retry_async(_do, timeout=timeout, retry_delay=retry_delay)


async def scp_download(
    remote_path: str,
    local_path: str,
    ip_address: str,
    *,
    user: str,
    key_path: str | None = None,
    retry_delay: int = 10,
    timeout: int = 120,
    recurse: bool = True,
):
    async def _do():
        await _wait_ssh_ready(ip_address, user, key_path, timeout, 3)
        Path(local_path).mkdir(parents=True, exist_ok=True)
        async with asyncssh.connect(ip_address, **_connect_kwargs(user, key_path)) as conn:
            await asyncssh.scp((conn, remote_path), local_path, recurse=recurse)

    await _retry_async(_do, timeout=timeout, retry_delay=retry_delay)
