from pathlib import Path
from time import sleep
from unittest import mock

import os
import time

from elastic_datashader import cache

import pytest

def test_du_fail(tmp_path):
    with pytest.raises(FileNotFoundError):
        cache.du(tmp_path / "foo" / "bar" / "baz")


def test_du(tmp_path):
    foo = tmp_path / "foo"
    foo.mkdir()
    bar = foo / "bar.txt"
    bar.write_text("Hello, World!")
    actual = cache.du(tmp_path)
    assert isinstance(actual, str)
    assert actual.endswith("B")  # bytes


def test_tile_name():
    assert cache.tile_name("abc", 1, 2, 3, "somehash") == "abc/somehash/3/1/2.png"


def test_tile_id():
    assert cache.tile_id("abc", 1, 2, 3, "somehash") == "abc_somehash_3_1_2"


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


def test_clear_hash_cache(tmp_path):
    idx_path = tmp_path / "fooindex"
    idx_path.mkdir()
    param_hash_path = idx_path / "somehash"
    param_hash_path.mkdir()
    bar = param_hash_path / "bar.txt"
    bar.write_text("the quick brown fox jumps over the lazy dog")
    cache.clear_hash_cache(tmp_path, "fooindex", "somehash")
    assert not bar.exists()
    assert not param_hash_path.exists()
    assert idx_path.exists()


def test_age_off_cache(tmp_path):
    xdir = tmp_path / "fooindex/somehash/3/1"
    xdir.mkdir(parents=True)

    yfile = xdir / "2.png"
    yfile.write_text("a picture as the quick brown fox jumps over the lazy dog")

    sleep(3)

    yfile_after = xdir / "3.png"
    yfile_after.write_text("a picture as the quick brown fox jumps over the lazy dog")

    cache.age_off_cache(tmp_path, "fooindex", 2)

    assert not yfile.exists()
    assert yfile_after.exists()


def test_build_layer_info(tmp_path):
    foo_idx_path = tmp_path / "foo"
    bar_idx_path = tmp_path / "bar"

    foo_somehash_path = foo_idx_path / "somehash"
    foo_otherhash_path = foo_idx_path / "otherhash"

    foo_somehash_file = foo_somehash_path / "fox.txt"
    foo_otherhash_file = foo_otherhash_path / "dog.txt"

    foo_somehash_path.mkdir(parents=True)
    foo_otherhash_path.mkdir(parents=True)

    foo_somehash_file.write_text("the quick brown fox jumps over the lazy dog")
    foo_otherhash_file.write_text("the quick brown dog jumps over the lazy fox")

    bar_idx_path.touch()  # file not directory, which should be skipped in output

    sleep(3)

    layer_info = cache.build_layer_info(tmp_path)
    assert layer_info["foo"]["somehash"]["age"].startswith("3")
    assert "B" in layer_info["foo"]["somehash"]["size"]
    assert layer_info["foo"]["otherhash"]["age"].startswith("3")
    assert "B" in layer_info["foo"]["otherhash"]["size"]
    assert layer_info.get("bar") is None
