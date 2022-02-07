from dataclasses import dataclass
from logging import getLevelName, INFO
from os import environ
from pathlib import Path
from socket import getfqdn
from ssl import PROTOCOL_TLSv1_2, SSLContext
from typing import Any, Dict, Optional, Union

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
    ssl_context: Optional[Union[SSLContext, str]]
    tms_key: Optional[str]
    use_scroll: bool

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

def get_ssl_context() -> Optional[Union[SSLContext, str]]:
    if environ.get("DATASHADER_SSL_ADHOC", None) is not None:
        return "adhoc"

    if environ.get("DATASHADER_SSL", None) is not None:
        context = SSLContext(PROTOCOL_TLSv1_2)
        context.load_verify_locations(environ.get("SSL_CA_CHAIN"))
        context.load_cert_chain(
            environ.get("SSL_SERVER_CERT"),
            environ.get("SSL_SERVER_KEY"),
        )
        return context

    return None

def check_config(config: Config) -> None:
    if not config.cache_path.exists():
        raise Exception(f"DATASHADER_CACHE_DIRECTORY '{config.cache_path}' does not exist")

    if not config.cache_path.is_dir():
        raise Exception(f"DATASHADER_CACHE_DIRECTORY '{config.cache_path}' is not a directory")

def config_from_env() -> Config:
    return Config(
        allowlist_headers=environ.get("DATASHADER_ALLOWLIST_HEADERS", None),
        cache_path=Path(environ.get("DATASHADER_CACHE_DIRECTORY", "tms-cache")),
        cache_timeout_seconds=int(environ.get("DATASHADER_CACHE_TIMEOUT", 60*60)),
        csrf_secret_key=environ.get("DATASHADER_CSRF_SECRET_KEY", "CSRFProtectionKey"),
        datashader_headers=load_datashader_headers(environ.get("DATASHADER_HEADER_FILE", "headers.yaml")),
        elastic_hosts=environ.get("DATASHADER_ELASTIC", "http://localhost:9200"),
        ellipse_render_mode=environ.get("DATASHADER_ELLIPSE_RENDER_MODE", "matrix"),
        hostname=getfqdn(),
        log_level=get_log_level(environ.get("DATASHADER_LOG_LEVEL", None)),
        max_batch=int(environ.get("DATASHADER_MAX_BATCH", 10_000)),
        max_bins=int(environ.get("DATASHADER_MAX_BINS", 10_000)),
        max_ellipses_per_tile=int(environ.get("DATASHADER_MAX_ELLIPSES_PER_TILE", 100_000)),
        max_legend_items_per_tile=int(environ.get("MAX_LEGEND_ITEMS_PER_TILE", 20)),
        num_ellipse_points=int(environ.get("DATASHADER_NUM_ELLIPSE_POINTS", 100)),
        proxy_host=environ.get("DATASHADER_PROXY_HOST", None),
        proxy_prefix=environ.get("DATASHADER_PROXY_PREFIX", ""),
        query_timeout_seconds=int(environ.get("DATASHADER_QUERY_TIMEOUT", 0)),
        ssl_context=get_ssl_context(),
        tms_key=environ.get("DATASHADER_TMS_KEY", None),
        use_scroll=(environ.get("DATASHADER_USE_SCROLL", None) is not None),
    )

config = config_from_env()
check_config(config)
