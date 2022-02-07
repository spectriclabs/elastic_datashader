from collections import OrderedDict
from os import scandir
from pathlib import Path
from typing import Dict, Optional

import json
import logging
import shutil
import time

from humanize import naturalsize

from .timeutil import pretty_time_delta

_log = logging.getLogger("apscheduler.scheduler.cache")
_log.addHandler(logging.NullHandler())

def tile_name(idx, x, y, z, parameter_hash) -> str:
    return f"{idx}/{parameter_hash}/{z}/{x}/{y}.png"

def tile_id(idx, x, y, z, parameter_hash) -> str:
    return "%s_%s_%s_%s_%s" % (idx, parameter_hash, z, x, y)

def directory_size(path: Path) -> int:
    '''
    Recursively traverses a directory to get the
    total size.  Note that os.scandir is used since
    it's iterable; it doesn't try to load all
    entries into memory at once.

    :param path: Get the size of directory at path
    :return: Directory size in bytes
    '''
    total = 0

    for entry in scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += directory_size(entry.path)

    return total

def du(path: Path) -> str:
    """Disk usage in human readable format (e.g. '2.1GB')

    :param path: Get the size of directory at path
    :return: Disk usage in human readable form
    """
    return naturalsize(directory_size(path), gnu=True)


def get_cache(cache_path: Path, tile: str) -> Optional[bytes]:
    """Retrieve data from the cache

    :param cache_path: Cache directory
    :param tile: Tile to attempt to retrieve
    :return: Tile from cache or None if not in cache
    """
    # Check if tile exists
    tile_path = cache_path / tile

    if tile_path.exists():
        return tile_path.read_bytes()

    return None

def set_cache(cache_path: Path, tile: str, img: bytes) -> None:
    """Add the tile image to the cache

    :param tile: Tile name
    :param img: Tile image data
    :param cache_path: Cache directory
    """
    tile_path = cache_path / tile

    # Make the directory if it doesn't already exist
    tile_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the file to the cache
    tile_path.write_bytes(img)

def check_cache_dir(cache_path: Path, layer_name: str) -> None:
    """
    Ensure the folder ``cache_path``/``layer_name`` exists

    :param cache_path: Top level directory
    :param layer_name: Specific layer in cache
    """
    tile_cache_path = cache_path / layer_name
    tile_cache_path.mkdir(parents=True, exist_ok=True)

def check_cache_age(cache_path: Path, age_limit: int) -> None:
    """Check for and delete any cache files older than ``age_limit``

    :param cache_path: Directory where the cache is (where the subdirectory
                      is layers)
    :param age_limit: The age limit in seconds above which to delete files
    """
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
                _log.info(
                    "Removing hash due to age: %s (%s>%s)",
                    hash_dir,
                    age_timestamp,
                    age_limit,
                )


def scheduled_cache_check_task(id_: str, cache_path: Path) -> None:
    """Cache check task callback that will be run every 5 minutes

    :param id_: Job thread ID
    :param cache_path: Cache directory to check
    """
    # See last update file
    _log.info("Checking for old cache %s (%s)", cache_path, id_)

    check_file = cache_path / "cache.age.check"

    # If the file doesn't exist, create it
    if not check_file.exists():
        _log.info("Had to recreate check file %s (%s)", cache_path, id_)
        check_file.touch()

    check_age = time.time() - check_file.stat().st_mtime
    _log.info("Checking age %s > %s (%s)", check_age, 300, id_)

    if check_age > 300:
        # Bump the utime
        check_file.touch(exist_ok=True)

        _log.info("Doing age check (%s)", id_)

        # Setup 24 hour cleanup (86400 == 24 * 60 * 60)
        check_cache_age(cache_path, 86400)

        _log.info("Cache check complete (%s)", id_)


def build_layer_info(cache_path: Path) -> Dict[str, OrderedDict]:
    """Build up dictionary of layer info

    :param cache_path: Cache directory
    :return: Dictionary containing parameters for each layer and hash
    """
    layer_info = {}
    for layer in cache_path.iterdir():
        # We only care if the layer isn't a file
        if layer.is_file():
            continue

        for hash_dir in layer.iterdir():
            params_json = hash_dir / "params.json"

            # We only care if params_json exists
            if not params_json.exists():
                continue

            with params_json.open("r") as f:
                params = json.load(f)

            # Check age of hash
            params["age_timestamp"] = params_json.stat().st_mtime
            params["age"] = pretty_time_delta(time.time() - params["age_timestamp"])
            # Check size of hash
            try:
                params["size"] = du(hash_dir)
            except OSError:
                params["size"] = "Error"
            layer_info.setdefault(layer.name, OrderedDict())
            layer_info[layer.name][hash_dir.name] = params

        # Order hashes based off age, newest to oldest
        if layer_info.get(layer.name):
            layer_info[layer.name] = OrderedDict(
                reversed(
                    sorted(
                        layer_info[layer.name].items(),
                        key=lambda x: x[1]["age_timestamp"],
                    )
                )
            )
    return layer_info
