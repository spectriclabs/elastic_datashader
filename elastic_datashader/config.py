from dataclasses import dataclass
from logging import getLevelName, INFO
from os import environ
from pathlib import Path
from socket import getfqdn
from typing import Any, Dict, Optional

import yaml

@dataclass(frozen=True)
class Config:
    allowlist_headers: Optional[str]
    cache_path: Path
    cache_timeout_seconds: int
    csrf_secret_key: str
    datashader_headers: Dict[Any, Any]
    elastic_hosts: str
    ellipse_render_mode: str
    hostname: str
    log_level: int
    max_batch: int
    max_bins: int
    max_ellipses_per_tile: int
    max_legend_items_per_tile: int
    num_ellipse_points: int
    proxy_host: Optional[str]
    proxy_prefix: str
    query_timeout_seconds: int
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
        raise Exception(f"Failed to load HEADER_FILE from {header_path_str}") from ex

    if type(loaded_yaml) is not dict:
        raise ValueError(f"HEADER_FILE YAML should be a dict mapping, but received {loaded_yaml}")

    return loaded_yaml

def get_log_level(level_name: Optional[str]) -> int:
    if level_name is None:
        return INFO

    level_value = getLevelName(level_name.upper())

    if type(level_value) is not int:
        raise Exception(f"Invalid logging level {level_name}")

    return level_value

def true_if_none(val: Optional[str]) -> bool:
    if val is None:
        return True

    if val.lower() in ("no", "false", "off"):
        return False

    return True

def check_config(c: Config) -> None:
    if not c.cache_path.exists():
        raise Exception(f"DATASHADER_CACHE_DIRECTORY '{c.cache_path}' does not exist")

    if not c.cache_path.is_dir():
        raise Exception(f"DATASHADER_CACHE_DIRECTORY '{c.cache_path}' is not a directory")

def config_from_env(env) -> Config:
    return Config(
        allowlist_headers=env.get("DATASHADER_ALLOWLIST_HEADERS", None),
        cache_path=Path(env.get("DATASHADER_CACHE_DIRECTORY", "tms-cache")),
        cache_timeout_seconds=int(env.get("DATASHADER_CACHE_TIMEOUT", 60*60)),
        csrf_secret_key=env.get("DATASHADER_CSRF_SECRET_KEY", "CSRFProtectionKey"),
        datashader_headers=load_datashader_headers(env.get("DATASHADER_HEADER_FILE", "headers.yaml")),
        elastic_hosts=env.get("DATASHADER_ELASTIC", "http://localhost:9200"),
        ellipse_render_mode=env.get("DATASHADER_ELLIPSE_RENDER_MODE", "matrix"),
        hostname=getfqdn(),
        log_level=get_log_level(env.get("DATASHADER_LOG_LEVEL", None)),
        max_batch=int(env.get("DATASHADER_MAX_BATCH", 10_000)),
        max_bins=int(env.get("DATASHADER_MAX_BINS", 10_000)),
        max_ellipses_per_tile=int(env.get("DATASHADER_MAX_ELLIPSES_PER_TILE", 100_000)),
        max_legend_items_per_tile=int(env.get("MAX_LEGEND_ITEMS_PER_TILE", 20)),
        num_ellipse_points=int(env.get("DATASHADER_NUM_ELLIPSE_POINTS", 100)),
        proxy_host=env.get("DATASHADER_PROXY_HOST", None),
        proxy_prefix=env.get("DATASHADER_PROXY_PREFIX", ""),
        query_timeout_seconds=int(env.get("DATASHADER_QUERY_TIMEOUT", 0)),
        tms_key=env.get("DATASHADER_TMS_KEY", None),
        use_scroll=true_if_none(env.get("DATASHADER_USE_SCROLL", None)),
        verify_indices=true_if_none(env.get("DATASHADER_VERIFY_INDICES", None)),
    )

config = config_from_env(environ)
check_config(config)
