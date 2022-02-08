from pathlib import Path
from unittest import mock

import os
import time

from elastic_datashader import cache

import pytest

def test_du_fail(tmp_path):
    with pytest.raises(FileNotFoundError):
        cache.du(tmp_path / "foo" / "bar" / "baz")

def test_du(tmp_path):
    foo = tmp_path / "foo.txt"
    foo.write_text("Hello, World!")
    actual = cache.du(tmp_path)
    assert isinstance(actual, str)
    assert actual.endswith("B")  # bytes


def test_get_cache_none():
    assert cache.get_cache(Path("/foo/bar"), "baz") is None


def test_get_cache(tmp_path):
    tile = "foo.png"
    foo_path = tmp_path / tile
    img = b"helloworld_get"
    foo_path.write_bytes(img)

    assert cache.get_cache(tmp_path, tile) == img


def test_set_cache(tmp_path):
    img = b"helloworld"
    tile = "settile.png"
    cache.set_cache(tmp_path, tile, img)

    tile_path = tmp_path / "settile.png"
    assert tile_path.exists()
    assert tile_path.read_bytes() == img


def test_check_cache_dir(tmp_path):
    cache.check_cache_dir(tmp_path, "foo")
    assert (tmp_path / "foo").exists()
