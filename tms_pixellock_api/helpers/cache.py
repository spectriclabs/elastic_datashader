#!/usr/bin/env python3
import logging
import shutil
import time
from pathlib import Path
from typing import Optional, Union


def get_cache(cache_dir: Union[Path, str], tile: str) -> Optional[bytes]:
    """Retrieve data from the cache

    :param cache_dir: Cache directory
    :param tile: Tile to attempt to retrieve
    :return: Tile from cache or None if not in cache
    """
    # Check if tile exists
    tile_path = Path(cache_dir) / tile
    if tile_path.exists():
        with tile_path.open("rb") as tile_data:
            return tile_data.read()
    return None


def set_cache(cache_dir: Union[Path, str], tile: str, img: bytes) -> None:
    """Add the tile image to the cache

    :param tile: Tile name
    :param img: Tile image data
    :param cache_dir: Cache directory
    """
    tile_path = Path(cache_dir) / tile

    # Make the directory if it doesn't already exist
    tile_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the file to the cache
    with tile_path.open("wb") as tile_file:
        tile_file.write(img)


def check_cache_dir(cache_dir: Union[str, Path], layer_name: str) -> None:
    """Ensure the folder ``cache_dir``/``layer_name`` exists

    :param cache_dir: Top level directory
    :param layer_name: Specific layer in cache
    """
    tile_cache_path = Path(cache_dir) / layer_name
    tile_cache_path.mkdir(parents=True, exist_ok=True)


def check_cache_age(cache_dir: Union[Path, str], age_limit: int) -> None:
    """Check for and delete any cache files older than ``age_limit``

    :param cache_dir: Directory where the cache is (where the subdirectory
                      is layers)
    :param age_limit: The age limit above which to delete files
    """
    cache_path = Path(cache_dir)
    for layer_dir in cache_path.iterdir():
        # Skip if ``layer_dir`` is a file
        if layer_dir.is_file():
            continue

        for hash_dir in layer_dir.iterdir():
            params_json = hash_dir / "params.json"

            # Skip if the params JSON file doesn't exist
            if not params_json.exists():
                continue

            # Check age of hash; if older than ``age_limit``, delete it
            age_timestamp = time.time() - params_json.stat().st_mtime
            if age_timestamp > age_limit:
                shutil.rmtree(hash_dir)
                logging.info(
                    "Removing hash due to age: %s (%s>%s)",
                    hash_dir,
                    age_timestamp,
                    age_limit,
                )


def scheduled_cache_check_task(id_: str, cache_dir: Union[Path, str]) -> None:
    """Cache check task callback that will be run every 5 minutes

    :param id_: Job thread ID
    :param cache_dir: Cache directory to check
    """
    # See last update file
    logging.info("Checking for old cache (%s) at %s", id_, cache_dir)

    cache_path = Path(cache_dir)
    check_file = cache_path / "cache.age.check"

    # If the file doesn't exist, create it
    if not check_file.exists():
        logging.info("Had to recreate check file (%s) at %s", id_, cache_dir)
        check_file.touch()

    check_age = time.time() - check_file.stat().st_mtime
    logging.info("Checking age(%s) at %s", id_, check_age)

    if check_age > 300:
        # Bump the utime
        check_file.touch(exist_ok=True)

        logging.info("Doing age check (%s)", id_)

        # Setup 24 hour cleanup (86400 == 24 * 60 * 60)
        check_cache_age(cache_dir, 86400)

        logging.info("Cache check complete (%s)", id_)
