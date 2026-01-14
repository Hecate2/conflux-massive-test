import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import asyncssh
import pytest

from conflux_deployer.utils.remote import RemoteExecutor


@dataclass
class _RunResult:
    exit_status: Optional[int]
    stdout: Any = ""
    stderr: Any = ""


class _FakeFile:
    def __init__(self, store: Dict[str, str], path: str):
        self._store = store
        self._path = path
        self._buf: List[str] = []

    async def write(self, data: str) -> None:
        self._buf.append(data)

    async def close(self) -> None:
        self._store[self._path] = "".join(self._buf)


class _FakeSFTP:
    def __init__(self, files: Dict[str, str], puts: List[Tuple[str, str]]):
        self._files = files
        self._puts = puts

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def put(self, local_path: str, remote_path: str) -> None:
        self._puts.append((local_path, remote_path))

    async def open(self, remote_path: str, mode: str):
        assert "w" in mode
        return _FakeFile(self._files, remote_path)


class _FakeConn:
    def __init__(
        self,
        run_results: Dict[str, _RunResult],
        files: Dict[str, str],
        puts: List[Tuple[str, str]],
        fail_run: bool = False,
    ):
        self._run_results = run_results
        self._files = files
        self._puts = puts
        self._fail_run = fail_run
        self.ran: List[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def run(self, command: str, check: bool = False):
        self.ran.append(command)
        if self._fail_run:
            raise asyncssh.DisconnectError(11, "boom")
        return self._run_results.get(command, _RunResult(exit_status=0, stdout="", stderr=""))

    def start_sftp_client(self):
        return _FakeSFTP(self._files, self._puts)


@pytest.fixture
def fake_asyncssh(monkeypatch):
    # Per-test state
    state = {
        "files": {},
        "puts": [],
        "conns": {},
    }

    async def fake_connect(host: str, **kwargs):
        conn = state["conns"].get(host)
        if conn is None:
            # Default: success conn
            conn = _FakeConn(run_results={}, files=state["files"], puts=state["puts"])
            state["conns"][host] = conn
        return conn

    monkeypatch.setattr(asyncssh, "connect", fake_connect)
    return state


def test_execute_on_host_success_text_and_return_code(fake_asyncssh):
    host = "1.2.3.4"
    cmd = "echo hi"

    fake_asyncssh["conns"][host] = _FakeConn(
        run_results={cmd: _RunResult(exit_status=0, stdout=b"ok\n", stderr=b"")},
        files=fake_asyncssh["files"],
        puts=fake_asyncssh["puts"],
    )

    ex = RemoteExecutor(ssh_key_path="/tmp/key", ssh_user="ubuntu")
    res = ex.execute_on_host(host, cmd)

    assert res.host == host
    assert res.success is True
    assert res.return_code == 0
    assert res.stdout == "ok\n"
    assert res.stderr == ""


def test_execute_on_host_failure_exit_status_none(fake_asyncssh):
    host = "1.2.3.4"
    cmd = "bad"

    fake_asyncssh["conns"][host] = _FakeConn(
        run_results={cmd: _RunResult(exit_status=None, stdout="", stderr="err")},
        files=fake_asyncssh["files"],
        puts=fake_asyncssh["puts"],
    )

    ex = RemoteExecutor()
    res = ex.execute_on_host(host, cmd)

    assert res.success is False
    assert res.return_code == -1


def test_execute_on_all_mixed_success_and_exception(fake_asyncssh):
    ok_host = "1.1.1.1"
    bad_host = "2.2.2.2"
    cmd = "uname -a"

    fake_asyncssh["conns"][ok_host] = _FakeConn(
        run_results={cmd: _RunResult(exit_status=0, stdout="linux", stderr="")},
        files=fake_asyncssh["files"],
        puts=fake_asyncssh["puts"],
    )
    fake_asyncssh["conns"][bad_host] = _FakeConn(
        run_results={},
        files=fake_asyncssh["files"],
        puts=fake_asyncssh["puts"],
        fail_run=True,
    )

    ex = RemoteExecutor()
    res = ex.execute_on_all([ok_host, bad_host], cmd, max_workers=10, retry=0, timeout=5)

    assert res[ok_host].success is True
    assert res[bad_host].success is False
    assert "boom" in res[bad_host].stderr


def test_copy_file_and_copy_file_to_all_calls_sftp_put(fake_asyncssh, tmp_path):
    local = tmp_path / "a.txt"
    local.write_text("hi")

    ex = RemoteExecutor()

    # single
    assert ex.copy_file("1.1.1.1", str(local), "/tmp/a.txt", retry=0) is True

    # fan-out
    res = ex.copy_file_to_all(["1.1.1.1", "2.2.2.2"], str(local), "/tmp/b.txt", max_workers=10, retry=0)
    assert res["1.1.1.1"] is True
    assert res["2.2.2.2"] is True

    # Ensure we recorded some puts
    assert (str(local), "/tmp/a.txt") in fake_asyncssh["puts"]
    assert (str(local), "/tmp/b.txt") in fake_asyncssh["puts"]


def test_copy_contents_writes_multiple_files(fake_asyncssh):
    host = "1.1.1.1"
    ex = RemoteExecutor()

    out = ex.copy_contents(
        host,
        {
            "/tmp/x.txt": "x",
            "/tmp/y.txt": "y",
        },
        retry=0,
        timeout=5,
    )

    assert out["/tmp/x.txt"] is True
    assert out["/tmp/y.txt"] is True
    assert fake_asyncssh["files"]["/tmp/x.txt"] == "x"
    assert fake_asyncssh["files"]["/tmp/y.txt"] == "y"


def test_execute_commands_on_hosts_batches_per_host(fake_asyncssh):
    host = "9.9.9.9"
    fake_asyncssh["conns"][host] = _FakeConn(
        run_results={
            "c1": _RunResult(exit_status=0, stdout="o1", stderr=""),
            "c2": _RunResult(exit_status=3, stdout="", stderr="e2"),
        },
        files=fake_asyncssh["files"],
        puts=fake_asyncssh["puts"],
    )

    ex = RemoteExecutor()
    res = ex.execute_commands_on_hosts({host: {"k1": "c1", "k2": "c2"}}, max_workers=5, retry=0, timeout=5)

    assert res[host]["k1"].success is True
    assert res[host]["k2"].success is False
    assert res[host]["k2"].return_code == 3

    # Ensure both commands ran on same connection
    assert fake_asyncssh["conns"][host].ran == ["c1", "c2"]
