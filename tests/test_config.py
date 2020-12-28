#!/usr/bin/env pytest
import os
import socket
import importlib


def test_config_defaults():
    from tms_datashader_api.helpers import config

    assert config.Config.LOG_LEVEL is None
    assert config.Config.CACHE_DIRECTORY == "./tms-cache/"
    assert config.Config.CACHE_TIMEOUT == 3600
    assert config.Config.ELASTIC == "http://localhost:9200"
    assert config.Config.PROXY_HOST is None
    assert config.Config.PROXY_PREFIX == ""
    assert config.Config.TMS_KEY is None
    assert config.Config.MAX_BINS == 10000
    assert config.Config.MAX_BATCH == 10000
    assert config.Config.MAX_ELLIPSES_PER_TILE == 100000
    assert config.Config.HEADER_FILE == "./headers.yaml"
    assert config.Config.WHITELIST_HEADERS is None
    assert config.Config.QUERY_TIMEOUT == 0
    assert config.Config.PORT is None
    assert config.Config.HOSTNAME == socket.getfqdn()


def test_config_env():
    os.environ.update(
        {
            "DATASHADER_LOG_LEVEL": "info",
            "DATASHADER_CACHE_DIRECTORY": "./tms-cache-foo/",
            "DATASHADER_CACHE_TIMEOUT": "60",
            "DATASHADER_ELASTIC": "http://localhost:9201",
            "DATASHADER_PROXY_HOST": "http://localhost:1337",
            "DATASHADER_PROXY_PREFIX": "foo",
            "DATASHADER_TMS_KEY": "bar",
            "DATASHADER_MAX_BINS": "10",
            "DATASHADER_MAX_BATCH": "1000",
            "DATASHADER_HEADER_FILE": "./headers-foo.yaml",
            "DATASHADER_WHITELIST_HEADERS": "blah",
            "DATASHADER_DEBUG_TILES": "True",
            "DATASHADER_QUERY_TIMEOUT": "1",
        }
    )
    from tms_datashader_api.helpers import config

    importlib.reload(config)

    assert config.Config.LOG_LEVEL == "info"
    assert config.Config.CACHE_DIRECTORY == "./tms-cache-foo/"
    assert config.Config.CACHE_TIMEOUT == 60
    assert config.Config.ELASTIC == "http://localhost:9201"
    assert config.Config.PROXY_HOST == "http://localhost:1337"
    assert config.Config.PROXY_PREFIX == "foo"
    assert config.Config.TMS_KEY == "bar"
    assert config.Config.MAX_BINS == 10
    assert config.Config.MAX_BATCH == 1000
    assert config.Config.MAX_ELLIPSES_PER_TILE == 100000
    assert config.Config.HEADER_FILE == "./headers-foo.yaml"
    assert config.Config.WHITELIST_HEADERS == "blah"
    assert config.Config.QUERY_TIMEOUT == 1
    assert config.Config.PORT is None
    assert config.Config.HOSTNAME == socket.getfqdn()
