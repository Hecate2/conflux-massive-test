"""Copy a server image from a source Aliyun region to target regions."""

# Allow running this file directly via `python auxiliary/ali_instances/image_copy.py`
if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    import sys
    from pathlib import Path as _Path

    sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))

import argparse
import time
import tomllib
from pathlib import Path
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from dotenv import load_dotenv
from alibabacloud_ecs20140526 import models as ecs_models

from cloud_provisioner.aliyun_provider.client_factory import AliyunClient


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "request_config.example.toml"


def _load_regions(config_path: Path) -> tuple[str, list[str]]:
    with open(config_path, "rb") as f:
        data = tomllib.load(f)
    aliyun_cfg = data.get("aliyun") or {}
    image_name = (aliyun_cfg.get("image_name") or "conflux-docker-registry").strip()
    regions_cfg = aliyun_cfg.get("regions") or []
    regions = []
    for region in regions_cfg:
        name = (region.get("name") or "").strip()
        if name:
            regions.append(name)
    return image_name, regions


def _wait_image_available(ecs, *, region_id: str, image_id: str, timeout: int = 3600, poll: int = 10) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = ecs.describe_images(ecs_models.DescribeImagesRequest(region_id=region_id, image_id=image_id))
        images = resp.body.images.image if resp.body and resp.body.images else []
        if not images:
            time.sleep(poll)
            continue
        state = images[0].status
        logger.info(f"image {image_id}: {state}")
        if state == "Available":
            return
        if state in {"CreateFailed", "UnAvailable", "Deprecated"}:
            raise RuntimeError(f"image copy failed: {state}")
        time.sleep(poll)
    raise RuntimeError(f"image {image_id} not available within timeout")


def _copy_to_regions(
    ali: AliyunClient,
    *,
    image_name: str,
    source_region: str,
    target_regions: Iterable[str],
) -> None:
    source_client = ali.build(source_region)
    source_images = ali.get_images_in_region(source_region, image_name)
    if not source_images:
        raise RuntimeError(f"image {image_name} not found in {source_region}")
    source_image_id = source_images[0].image_id
    logger.info(f"source image: {image_name} {source_image_id} in {source_region}")

    # Perform region copies in parallel and skip targets that already have an image
    tasks = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(target_regions)))) as executor:
        for region in target_regions:
            if region == source_region:
                logger.info(f"skip region {region}")
                continue
            # Skip if an image with the same name exists in this region (any status)
            existing = ali.get_images_in_region(region, image_name)
            if existing:
                logger.info(f"image already exists in {region}: {existing[0].image_id}")
                continue
            # Check that region exposes zones before copying
            try:
                zones = ali.get_zone_ids_in_region(region)
                if not zones:
                    logger.warning(f"region {region} has no zones, skipping")
                    continue
            except Exception as exc:
                logger.warning(f"failed to query zones for {region}, skipping copy: {exc}")
                continue

            logger.info(f"scheduling copy to {region}")

            def _copy_into_region(region=region):
                # Use fresh client handles in each thread
                source_client_local = ali.build(source_region)
                resp = source_client_local.copy_image(
                    ecs_models.CopyImageRequest(
                        region_id=source_region,
                        destination_region_id=region,
                        image_id=source_image_id,
                        destination_image_name=image_name,
                    )
                )
                image_id = resp.body.image_id if resp.body else None
                if not image_id:
                    raise RuntimeError(f"copy_image did not return image_id for {region}")
                target_client_local = ali.build(region)
                _wait_image_available(target_client_local, region_id=region, image_id=image_id)
                logger.success(f"image available in {region}: {image_id}")
                return region, image_id

            tasks.append(executor.submit(_copy_into_region))

    for fut in as_completed(tasks):
        try:
            region, image_id = fut.result()
        except Exception as exc:
            logger.error(f"image copy task failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy Aliyun image to target regions")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to request_config.example.toml")
    parser.add_argument("--source", default="ap-southeast-3", help="Source region")
    args = parser.parse_args()

    config_path = Path(args.config).expanduser()
    image_name, regions = _load_regions(config_path)
    if not regions:
        raise RuntimeError("no aliyun regions found in config")

    # Load credentials from .env if present
    load_dotenv()
    ali = AliyunClient.load_from_env()
    _copy_to_regions(
        ali,
        image_name=image_name,
        source_region=args.source,
        target_regions=regions,
    )


if __name__ == "__main__":
    main()
