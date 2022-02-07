from logging import INFO
from pathlib import Path

import os
import socket

def test_config_defaults():
    from elastic_datashader.config import config

    assert config.log_level is INFO
    assert config.cache_path == Path("tms-cache")
    assert config.cache_timeout_seconds == 3600
    assert config.elastic_hosts == "http://localhost:9200"
    assert config.proxy_host is None
    assert config.proxy_prefix == ""
    assert config.tms_key is None
    assert config.max_bins == 10000
    assert config.max_batch == 10000
    assert config.max_ellipses_per_tile == 100000
    assert config.allowlist_headers is None
    assert config.query_timeout_seconds == 0
    assert config.hostname == socket.getfqdn()


def test_config_env():
    os.environ.update({
        "DATASHADER_LOG_LEVEL": "info",
        "DATASHADER_CACHE_DIRECTORY": "tms-cache-foo",
        "DATASHADER_CACHE_TIMEOUT": "60",
        "DATASHADER_ELASTIC": "http://localhost:9201",
        "DATASHADER_PROXY_HOST": "http://localhost:1337",
        "DATASHADER_PROXY_PREFIX": "foo",
        "DATASHADER_TMS_KEY": "bar",
        "DATASHADER_MAX_BINS": "10",
        "DATASHADER_MAX_BATCH": "1000",
        "DATASHADER_ALLOWLIST_HEADERS": "blah",
        "DATASHADER_DEBUG_TILES": "True",
        "DATASHADER_QUERY_TIMEOUT": "1",
    })

    from elastic_datashader.config import config

    assert config.log_level == INFO
    assert config.cache_path == Path("tms-cache-foo")
    assert config.cache_timeout_seconds== 60
    assert config.elastic_hosts == "http://localhost:9201"
    assert config.proxy_host == "http://localhost:1337"
    assert config.proxy_prefix == "foo"
    assert config.tms_key == "bar"
    assert config.max_bins == 10
    assert config.max_batch == 1000
    assert config.max_ellipses_per_tile == 100000
    assert config.allowlist_headers == "blah"
    assert config.query_timeout_seconds == 1
    assert config.hostname == socket.getfqdn()
