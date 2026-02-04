"""Host spec model for provisioned instances."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import List, Optional


@dataclass
class HostSpec:
    ip: str
    nodes_per_host: int
    ssh_user: str = "root"
    ssh_key_path: Optional[str] = None
    provider: Optional[str] = None
    region: Optional[str] = None
    instance_id: Optional[str] = None

    
def save_hosts(hosts: List[HostSpec], file_path: str):
    json.dump([asdict(host) for host in hosts], open(file_path, "w"), ensure_ascii=True, indent=2)
    
def load_hosts(file_path: str) -> List[HostSpec]:
    data = json.load(open(file_path, "r"))
    return [HostSpec(**item) for item in data]