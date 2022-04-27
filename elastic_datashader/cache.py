from asyncio import sleep
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from os import scandir
from pathlib import Path
from shutil import rmtree
from time import time
from typing import Dict, Iterable, Optional

from humanize import naturalsize

from .config import config
from .logger import logger
from .timeutil import pretty_time_delta

def path_age(now: datetime, path: Path) -> timedelta:
    try:
        path_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except FileNotFoundError:
        return timedelta(seconds=0)

    return now - path_dt

def tile_name(idx, x, y, z, parameter_hash) -> str:
    return f"{idx}/{parameter_hash}/{z}/{x}/{y}.png"

def rendering_tile_name(idx, x, y, z, parameter_hash) -> str:
    return f"{idx}/{parameter_hash}/{z}/{x}/{y}.rendering"

def tile_id(idx, x, y, z, parameter_hash) -> str:
    return f"{idx}_{parameter_hash}_{z}_{x}_{y}"

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

def cache_entry_exists(cache_path: Path, tile: str) -> bool:
    tile_path = cache_path / tile

    if not tile_path.exists():
        return False

    if path_age(datetime.now(timezone.utc), tile_path) > config.cache_timeout:
        return False

    return True

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

def claim_cache_placeholder(cache_path: Path, tile: str) -> bool:
    """
    Adds an empty placeholder file to the cache to
    claim the associated task.
    Returns True if this call created the file.
    Returns False if the file already existed.
    """
    tile_path = cache_path / tile
    tile_path.parent.mkdir(parents=True, exist_ok=True)

    # Return false if the placeholder was already set by another process,
    # but don't worry about it if the placeholder is old.
    try:
        if path_age(datetime.now(timezone.utc), tile_path) > config.render_timeout:
            tile_path.touch(exist_ok=True)
        else:
            tile_path.touch(exist_ok=False)

    except FileExistsError:
        return False

    return True

def cache_placeholder_exists(cache_path: Path, tile: str) -> None:
    tile_path = cache_path / tile
    return tile_path.exists()

def release_cache_placeholder(cache_path: Path, tile: str) -> None:
    tile_path = cache_path / tile

    if tile_path.exists():
        tile_path.unlink(missing_ok=True)

def check_cache_dir(cache_path: Path, layer_name: str) -> None:
    """
    Ensure the folder ``cache_path``/``layer_name`` exists

    :param cache_path: Top level directory
    :param layer_name: Specific layer in cache
    """
    tile_cache_path = cache_path / layer_name
    tile_cache_path.mkdir(parents=True, exist_ok=True)

def clear_hash_cache(cache_path: Path, idx_name: str, param_hash: Optional[str]) -> None:
    target_path = cache_path / idx_name

    if param_hash:
        target_path = target_path / param_hash

    if target_path.exists():
        rmtree(target_path, ignore_errors=True)

def age_off_cache(cache_path: Path, idx_name: str, max_age: timedelta) -> None:
    file_paths = cache_path.glob(f'{idx_name}/*/*/*/*.png')  # idx/hash/z/x/y.png

    for file_path in file_paths:
        file_age = path_age(datetime.now(timezone.utc), file_path)

        if file_age > max_age:
            logger.info("Aging off %s at %d sec old", file_path, file_age.total_seconds())
            # set missing_ok=True in case another process deleted the same file
            file_path.unlink(missing_ok=True)

def get_idx_names(cache_path: Path) -> Iterable[str]:
    for path in cache_path.glob("*"):
        if path.is_dir():
            yield path.name

async def background_cache_cleanup():
    while True:
        try:
            logger.info("Starting background cache cleanup")
            cache_cleanup_start = time()

            for idx_name in get_idx_names(config.cache_path):
                age_off_cache(config.cache_path, idx_name, config.cache_timeout)

            cache_cleanup_end = time()
            logger.info("Finished background cache cleanup in %ss", cache_cleanup_end-cache_cleanup_start)
            await sleep(config.cache_cleanup_interval.total_seconds())

        except Exception as ex:  # pylint: disable=W0703
            # ensure this loop never dies
            logger.error(str(ex))

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
            params["age"] = pretty_time_delta(seconds=time()-params["age_timestamp"])

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
