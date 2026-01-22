"""Host spec model for provisioned instances."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class HostSpec:
    ip: str
    nodes_per_host: int
    ssh_user: str = "root"
    ssh_key_path: Optional[str] = None
    provider: Optional[str] = None
    region: Optional[str] = None
    instance_id: Optional[str] = None
