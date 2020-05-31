#!/usr/bin/env pytest
import os
import subprocess
import time
from unittest import mock
import pytest
from tms_datashader_api.helpers import cache


def test_du_fail(tmp_path):
    with pytest.raises(subprocess.CalledProcessError):
        cache.du(tmp_path / "foo" / "bar" / "baz")


def test_du(tmp_path):
    foo = tmp_path / "foo.txt"
    foo.write_text("Hello, World!")
    actual = cache.du(foo)
    assert isinstance(actual, str)
    assert actual.endswith(".0K")


def test_get_cache_none():
    assert cache.get_cache("/foo/bar", "baz") is None


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


def test_check_cache_age(tmp_path):
    (tmp_path / "foo.txt").write_text("Hello, world!")

    cache_path = tmp_path / "layer1"
    cache_path.mkdir()

    (cache_path / "bar").mkdir()
    (cache_path / "baz").mkdir()
    (cache_path / "baz/params.json").touch()
    (cache_path / "hello").mkdir()
    (cache_path / "hello/params.json").touch()
    now = time.time()
    os.utime(str(cache_path / "hello/params.json"), (now, now - 3600))

    cache.check_cache_age(tmp_path, 300)

    assert (tmp_path / "foo.txt").exists()
    assert (cache_path / "bar").exists()
    assert (cache_path / "baz/params.json").exists()
    assert not (cache_path / "hello").exists()


@mock.patch("tms_datashader_api.helpers.cache.check_cache_age")
def test_scheduled_cache_check_task_file_nexist(check_cache_age_mock, tmp_path):
    cache.scheduled_cache_check_task("random_id", tmp_path)

    assert (tmp_path / "cache.age.check").exists()
    assert not check_cache_age_mock.called


@mock.patch("tms_datashader_api.helpers.cache.check_cache_age")
def test_scheduled_cache_check_task_file_exist_new(check_cache_age_mock, tmp_path):
    cache_check_path = tmp_path / "cache.age.check"
    cache_check_path.touch()
    cache.scheduled_cache_check_task("random_id", tmp_path)

    assert cache_check_path.exists()
    assert not check_cache_age_mock.called


@mock.patch("tms_datashader_api.helpers.cache.check_cache_age")
def test_scheduled_cache_check_task_file_exist_old(check_cache_age_mock, tmp_path):
    cache_check_path = tmp_path / "cache.age.check"
    cache_check_path.touch()
    now = time.time()
    os.utime(str(cache_check_path), (now, now - 301))
    cache.scheduled_cache_check_task("random_id", tmp_path)

    assert cache_check_path.exists()
    assert check_cache_age_mock.called
