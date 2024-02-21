from dataclasses import dataclass
from datetime import timedelta
from logging import getLevelName, INFO
from os import environ
from pathlib import Path
from re import compile as compile_regex
from socket import getfqdn
from typing import Any, Dict, Optional

import yaml

BASE64_PATTERN = compile_regex("([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?")

@dataclass(frozen=True)
class Config:
    allowlist_headers: Optional[str]
    api_key: Optional[str]
    cache_cleanup_interval: timedelta
    cache_path: Path
    cache_timeout: timedelta
    datashader_headers: Dict[Any, Any]
    elastic_hosts: str
    ellipse_render_mode: str
    ellipse_render_min_zoom: int
    hostname: str
    log_level: int
    max_batch: int
    max_bins: int
    max_ellipses_per_tile: int
    max_legend_items_per_tile: int
    num_ellipse_points: int
    query_timeout_seconds: int
    render_timeout: timedelta
    tms_key: Optional[str]
    use_scroll: bool
    verify_indices: bool

def load_datashader_headers(header_path_str: str) -> Dict[Any, Any]:
    header_path = Path(header_path_str)

    if not header_path.exists():
        return {}

    try:
        loaded_yaml = yaml.safe_load(header_path.read_text(encoding='utf8'))
    except (OSError, IOError, yaml.YAMLError) as ex:
        raise IOError(f"Failed to load HEADER_FILE from {header_path_str}") from ex

    if type(loaded_yaml) is not dict:
        raise ValueError(f"HEADER_FILE YAML should be a dict mapping, but received {loaded_yaml}")

    return loaded_yaml

def get_log_level(level_name: Optional[str]) -> int:
    if level_name is None:
        return INFO

    level_value = getLevelName(level_name.upper())

    if type(level_value) is not int:
        raise ValueError(f"Invalid logging level {level_name}")

    return level_value

def true_if_none(val: Optional[str]) -> bool:
    if val is None:
        return True

    if val.lower() in ("no", "false", "off"):
        return False

    return True

def is_base64_encoded(value: str) -> bool:
    return BASE64_PATTERN.fullmatch(value) is not None

def check_config(c: Config) -> None:
    if not c.cache_path.exists():
        raise IOError(f"DATASHADER_CACHE_DIRECTORY '{c.cache_path}' does not exist")

    if not c.cache_path.is_dir():
        raise IOError(f"DATASHADER_CACHE_DIRECTORY '{c.cache_path}' is not a directory")

    if c.api_key and not is_base64_encoded(c.api_key):
        raise ValueError(f"DATASHADER_ELASTIC_API_KEY '{c.api_key}' does not appear to be base64 encoded")

def config_from_env(env) -> Config:
    return Config(
        allowlist_headers=env.get("DATASHADER_ALLOWLIST_HEADERS", None),
        api_key=env.get("DATASHADER_ELASTIC_API_KEY", None),
        cache_cleanup_interval=timedelta(seconds=int(env.get("DATASHADER_CACHE_CLEANUP_INTERVAL", 5*60))),
        cache_path=Path(env.get("DATASHADER_CACHE_DIRECTORY", "tms-cache")),
        cache_timeout=timedelta(seconds=int(env.get("DATASHADER_CACHE_TIMEOUT", 60*60))),
        datashader_headers=load_datashader_headers(env.get("DATASHADER_HEADER_FILE", "headers.yaml")),
        elastic_hosts=env.get("DATASHADER_ELASTIC", "http://localhost:9200"),
        ellipse_render_mode=env.get("DATASHADER_ELLIPSE_RENDER_MODE", "matrix"),
        ellipse_render_min_zoom=env.get("DATASHADER_ELLIPSE_RENDER_MIN_ZOOM", 8),
        hostname=getfqdn(),
        log_level=get_log_level(env.get("DATASHADER_LOG_LEVEL", None)),
        max_batch=int(env.get("DATASHADER_MAX_BATCH", 10_000)),
        max_bins=int(env.get("DATASHADER_MAX_BINS", 10_000)),
        max_ellipses_per_tile=int(env.get("DATASHADER_MAX_ELLIPSES_PER_TILE", 100_000)),
        max_legend_items_per_tile=int(env.get("MAX_LEGEND_ITEMS_PER_TILE", 20)),
        num_ellipse_points=int(env.get("DATASHADER_NUM_ELLIPSE_POINTS", 100)),
        query_timeout_seconds=int(env.get("DATASHADER_QUERY_TIMEOUT", 900)),
        render_timeout=timedelta(seconds=int(env.get("DATASHADER_RENDER_TIMEOUT", 30))),
        tms_key=env.get("DATASHADER_TMS_KEY", None),
        use_scroll=true_if_none(env.get("DATASHADER_USE_SCROLL", None)),
        verify_indices=true_if_none(env.get("DATASHADER_VERIFY_INDICES", None)),
    )

config = config_from_env(environ)
check_config(config)
