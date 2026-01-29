from dataclasses import dataclass
import os
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config as AliyunConfig


@dataclass
class ClientFactory:
    access_key_id: str
    access_key_secret: str

    @classmethod
    def load_from_env(cls) -> 'ClientFactory':
        access_key_id = os.environ["ALI_ACCESS_KEY_ID"]
        access_key_secret = os.environ["ALI_ACCESS_KEY_SECRET"]
        return ClientFactory(access_key_id=access_key_id, access_key_secret=access_key_secret)

    def build(self, region_id: str) -> EcsClient:
        return EcsClient(
            AliyunConfig(
                access_key_id=self.access_key_id,
                access_key_secret=self.access_key_secret,
                region_id=region_id,
                read_timeout=120_000,
                connect_timeout=120_000
            )
        )