from datetime import timedelta
from logging import INFO
from pathlib import Path

import os
import socket

import pytest

import elastic_datashader.config as config

def test_config_defaults():
    cfg = config.config_from_env({})
    assert cfg.log_level is INFO
    assert cfg.cache_path == Path("tms-cache")
    assert cfg.cache_timeout == timedelta(seconds=3600)
    assert cfg.elastic_hosts == "http://localhost:9200"
    assert cfg.proxy_host is None
    assert cfg.proxy_prefix == ""
    assert cfg.tms_key is None
    assert cfg.max_bins == 10000
    assert cfg.max_batch == 10000
    assert cfg.max_ellipses_per_tile == 100000
    assert cfg.allowlist_headers is None
    assert cfg.query_timeout_seconds == 0
    assert cfg.hostname == socket.getfqdn()


def test_is_base64_encoded():
    assert config.is_base64_encoded("aVZlLUMzSUJuYndxdDJvN0k1bU46aGxlYUpNS2lTa2FKeVZua1FnY1VEdw==")
    assert config.is_base64_encoded("abcd")
    assert not config.is_base64_encoded("foo")
    assert not config.is_base64_encoded("abcd=")


def test_config_env():
    env = {
        "DATASHADER_ELASTIC_API_KEY": "aVZlLUMzSUJuYndxdDJvN0k1bU46aGxlYUpNS2lTa2FKeVZua1FnY1VEdw==",
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
    }

    cfg = config.config_from_env(env)
    assert cfg.api_key == "aVZlLUMzSUJuYndxdDJvN0k1bU46aGxlYUpNS2lTa2FKeVZua1FnY1VEdw=="
    assert cfg.log_level == INFO
    assert cfg.cache_path == Path("tms-cache-foo")
    assert cfg.cache_timeout == timedelta(seconds=60)
    assert cfg.elastic_hosts == "http://localhost:9201"
    assert cfg.proxy_host == "http://localhost:1337"
    assert cfg.proxy_prefix == "foo"
    assert cfg.tms_key == "bar"
    assert cfg.max_bins == 10
    assert cfg.max_batch == 1000
    assert cfg.max_ellipses_per_tile == 100000
    assert cfg.allowlist_headers == "blah"
    assert cfg.query_timeout_seconds == 1
    assert cfg.hostname == socket.getfqdn()

def test_get_log_level():
    with pytest.raises(Exception):
        config.get_log_level("foo")

def test_true_if_none():
    assert config.true_if_none(None) == True
    assert config.true_if_none("off") == False
    assert config.true_if_none("on") == True

def test_check_config_path(tmp_path):
    cache_path = tmp_path / "foo"
    cfg = config.config_from_env({"DATASHADER_CACHE_DIRECTORY": cache_path})

    with pytest.raises(Exception):
        config.check_config(cfg)

    cache_path.touch()  # make file not directory

    with pytest.raises(Exception):
        config.check_config(cfg)

def test_check_config_api_key(tmp_path):
    cache_path = tmp_path / "foo"
    cache_path.mkdir()
    cfg = config.config_from_env({
        "DATASHADER_CACHE_DIRECTORY": cache_path,
        "DATASHADER_ELASTIC_API_KEY": "foo",
    })

    with pytest.raises(Exception):
        config.check_config(cfg)

def test_load_datashader_headers(tmp_path):
    yaml_path = tmp_path / "foo.yaml"
    assert len(config.load_datashader_headers(str(yaml_path))) == 0

    yaml_path.write_text("""
=invalid
:yaml
""")

    with pytest.raises(Exception):
        config.load_datashader_headers(str(yaml_path))

    yaml_path.write_text("""
- foo
- bar
- baz
""")

    with pytest.raises(Exception):
        config.load_datashader_headers(str(yaml_path))

    yaml_path.write_text("""
foo: 1
bar: 2
baz: 3
""")

    loaded_yaml = config.load_datashader_headers(str(yaml_path))
    assert "foo" in loaded_yaml
