from collections import OrderedDict
from os import scandir
from pathlib import Path
from time import time
from typing import Dict, Optional

import logging

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

def age_off_cache(cache_path: Path, max_age_seconds: int) -> None:
    file_paths = cache_path.glob('*/*/*/*/*.png')  # idx/hash/z/x/y.png

    for file_path in file_paths:
        file_age = time() - file_path.stat().st_mtime

        if file_age > max_age_seconds:
            logging.info("Aging off %s at %d sec old", file_path, file_age)
            # set missing_ok=True in case another process deleted the same file
            file_path.unlink(missing_ok=True)

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

        params = {}

        for hash_dir in layer.iterdir():
            # Check age of hash
            params["age_timestamp"] = hash_dir.stat().st_mtime
            params["age"] = pretty_time_delta(time() - params["age_timestamp"])

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
