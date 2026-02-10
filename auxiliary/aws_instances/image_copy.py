"""Copy a server image from a source AWS region to target regions."""

# Allow running this file directly via `python auxiliary/aws_instances/image_copy.py`
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

from cloud_provisioner.aws_provider.client_factory import AwsClient


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "request_config.example.toml"


def _load_regions(config_path: Path) -> tuple[str, list[str]]:
    with open(config_path, "rb") as f:
        data = tomllib.load(f)
    aws_cfg = data.get("aws") or {}
    image_name = (aws_cfg.get("image_name") or "conflux-docker-registry").strip()
    regions_cfg = aws_cfg.get("regions") or []
    regions = []
    for region in regions_cfg:
        name = (region.get("name") or "").strip()
        if name:
            regions.append(name)
    return image_name, regions


def _wait_image_available(ec2, image_id: str, *, timeout: int = 3600, poll: int = 10) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = ec2.describe_images(ImageIds=[image_id])
        images = resp.get("Images", [])
        if not images:
            time.sleep(poll)
            continue
        state = images[0].get("State")
        logger.info(f"image {image_id}: {state}")
        if state == "available":
            return
        if state in {"failed", "deregistered"}:
            raise RuntimeError(f"image copy failed: {state}")
        time.sleep(poll)
    raise RuntimeError(f"image {image_id} not available within timeout")


def _copy_to_regions(
    aws: AwsClient,
    *,
    image_name: str,
    source_region: str,
    target_regions: Iterable[str],
    skip_regions: set[str],
) -> None:
    source_images = aws.get_images_in_region(source_region, image_name)
    if not source_images:
        raise RuntimeError(f"image {image_name} not found in {source_region}")
    source_image_id = source_images[0].image_id
    logger.info(f"source image: {image_name} {source_image_id} in {source_region}")

    # Launch parallel copies across regions when needed
    tasks = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(target_regions)))) as executor:
        for region in target_regions:
            if region in skip_regions or region == source_region:
                logger.info(f"skip region {region}")
                continue
            # Skip if an image with the same name exists in the target region (any status)
            existing = aws.get_images_in_region(region, image_name)
            if existing:
                logger.info(f"image already exists in {region}: {existing[0].image_id}")
                continue
            # Verify region has availability zones before attempting copy
            try:
                zones = aws.get_zone_ids_in_region(region)
                if not zones:
                    logger.warning(f"region {region} has no zones, skipping")
                    continue
            except Exception as exc:
                logger.warning(f"failed to query zones for {region}, skipping copy: {exc}")
                continue

            logger.info(f"scheduling copy to {region}")

            def _copy_into_region(region=region):
                ec2 = aws.build(region)
                resp = ec2.copy_image(SourceRegion=source_region, SourceImageId=source_image_id, Name=image_name)
                image_id = resp.get("ImageId")
                if not image_id:
                    raise RuntimeError(f"copy_image did not return image_id for {region}")
                _wait_image_available(ec2, image_id)
                logger.success(f"image available in {region}: {image_id}")
                return region, image_id

            tasks.append(executor.submit(_copy_into_region))

    # Collect task results and report failures
    for fut in as_completed(tasks):
        try:
            region, image_id = fut.result()
        except Exception as exc:
            logger.error(f"image copy task failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy AWS AMI to target regions")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to request_config.example.toml")
    parser.add_argument("--source", default="af-south-1", help="Source region")
    parser.add_argument("--exclude", action="append", default=["us-west-2"], help="Region to skip")
    args = parser.parse_args()

    config_path = Path(args.config).expanduser()
    image_name, regions = _load_regions(config_path)
    if not regions:
        raise RuntimeError("no aws regions found in config")

    skip = set(args.exclude or [])
    # Load credentials from .env if present
    load_dotenv()
    aws = AwsClient.new()
    _copy_to_regions(
        aws,
        image_name=image_name,
        source_region=args.source,
        target_regions=regions,
        skip_regions=skip,
    )


if __name__ == "__main__":
    main()
