from pathlib import Path

import json

import pytest

from fastapi.testclient import TestClient

from elastic_datashader.main import app
from elastic_datashader.cache import du

client = TestClient(app)

def setup_cache(cache_path: Path):
    """Helper method to setup cache directory for tests"""
    hash_dir = cache_path / "foo" / "abcdef"
    hash_dir.mkdir(exist_ok=True, parents=True)
    (hash_dir / "params.json").write_text(json.dumps({"foo": "bar", "baz": 300}))

def test_index_no_cache():
    with pytest.raises(FileNotFoundError):
        _ = client.get("/")

def test_index_cache(tmp_path):
    cache_path = tmp_path / "tms_cache"
    setup_cache(cache_path)
    size = du(cache_path)

    rv = client.get("/")
    assert size.encode("utf8") in rv.data
    assert b"abcdef" in rv.data
    assert b"foo" in rv.data

def test_display_parameters_no_params_json():
    rv = client.get("/parameters?name=foo&hash=abcdef")
    expected = b"""<html>
    <head>
      <title>DataShader Tile Map Server</title>
      <meta http-equiv="refresh" content="60" >
    </head>
    <body>
        <h2>DataShader Tile Map Server</h2>    
        <a href="./index">Home</a> | Total Cache Size  | Clean Cache <a href="./age_cache?age=3600">1hr</a> <a href="./age_cache?age=300">5min</a>
        <hr>
        

<h2>index:foo / hash:abcdef</h2>
<table>
  <style>
    table, th, td {
      border: 1px solid black;
    }
  </style>
  <tr>
    <th>Parameter</th>
    <th>Value</th>
  </tr>
  
</table>

    </body>
</html>"""
    assert expected == rv.data


def test_display_parameters_with_params_json(tmp_path):
    cache_path = tmp_path / "tms_cache"
    setup_cache(cache_path)

    rv = client.get("/parameters?name=foo&hash=abcdef")
    assert b"foo" in rv.data
    assert b"bar" in rv.data
    assert b"baz" in rv.data
    assert b"300" in rv.data
