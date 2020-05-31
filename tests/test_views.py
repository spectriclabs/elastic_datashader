#!/usr/bin/env pytest
import json
import subprocess
from pathlib import Path

import pytest

from tms_datashader import create_app


def setup_cache(cache_path: Path):
    hash_dir = cache_path / "foo" / "abcdef"
    hash_dir.mkdir(exist_ok=True, parents=True)
    with (hash_dir / "params.json").open("w") as f:
        json.dump({"foo": "bar", "baz": 300}, f)


@pytest.fixture
def client_and_cache(tmp_path):
    cache_path = tmp_path / "tms_cache"
    app = create_app(verify_indices=False)
    app.config.update(
        {
            "TESTING": True,
            "ELASTIC_DATASHADER_SETTINGS": "",
            "LOG_LEVEL": "info",
            "CACHE_DIRECTORY": str(cache_path)
        }
    )

    with app.test_client() as client:
        yield client, cache_path


def test_index_no_cache(client_and_cache):
    client, _ = client_and_cache
    with pytest.raises(subprocess.CalledProcessError):
        _ = client.get("/")


def test_index_cache(client_and_cache):
    client, cache_path = client_and_cache
    setup_cache(cache_path)

    rv = client.get("/")
    expected = b"""<html>
    <head>
      <title>DataShader Tile Map Server</title>
      <meta http-equiv="refresh" content="60" >
    </head>
    <body>
        <h2>DataShader Tile Map Server</h2>    
        <a href="./index">Home</a> | Total Cache Size 4.0K | Clean Cache <a href="./age_cache?age=3600">1hr</a> <a href="./age_cache?age=300">5min</a>
        <hr>
        
<table>
    <style>
        table, th, td {
            border: 1px solid black;
        }
    </style>
<tr>
    <th></th>
    <th>Index Name / Hash</th>
    <th>User</th>
    <th>Size</th>
    <th>Age</th>
    <th>Start</th>
    <th>Stop</th>
    <th>Category</th>
    <th>Ellipse</th>
    <th>Params</th>
</tr>

<tr>
    <td><a href="./clear_cache?name=foo">Clear</a></td>
    <td>foo</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
</tr>

<tr>
    <td><a href="./clear_cache?name=foo&hash=abcdef">Clear</a></td>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;abcdef</td>
    <td></td> 
    <td>4.0K</td>
    <td>0s</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td><a href="./parameters?name=foo&hash=abcdef">Params</a></td>
</tr>


</table>


    </body>
</html>"""
    assert expected == rv.data


def test_display_parameters_no_params_json(client_and_cache):
    client, cache_path = client_and_cache
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


def test_display_parameters_with_params_json(client_and_cache):
    client, cache_path = client_and_cache
    setup_cache(cache_path)

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
  
  <tr>
    <td>foo</td>
    <td>bar</td>
  </tr>
  
  <tr>
    <td>baz</td>
    <td>300</td>
  </tr>
  
</table>

    </body>
</html>"""
    assert expected == rv.data
