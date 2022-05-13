from datetime import datetime, timezone

import pytest

from elastic_datashader import parameters

def test_create_default_params():
    default = parameters.create_default_params()
    fields = (
        "ellipse_major",
        "ellipse_minor",
        "ellipse_tilt",
        "ellipse_units",
    )

    for field in fields:
        assert field in default

def test_normalize_spread():
    assert parameters.normalize_spread("coarse") == 10
    assert parameters.normalize_spread("fine") == 3
    assert parameters.normalize_spread("finest") == 1
    assert parameters.normalize_spread("auto") is None
    assert parameters.normalize_spread("42") == 42
    assert parameters.normalize_spread("foo") is None

def test_load_params_param():
    assert parameters.load_params_param(None) is None

    params = parameters.load_params_param("%7B%22foo%22%3A%201%2C%20%22bar%22%3A%20%22a%22%7D")
    assert params.get("foo", None) == 1
    assert params.get("bar", None) == "a"
    assert params.get("baz", None) is None

def test_get_from_time():
    assert parameters.get_from_time(None) is None
    assert parameters.get_from_time({"timeFilters": {"from": "42"}}) == "42"
    assert parameters.get_from_time({"timeFilters": {"foo": "bar"}}) is None

def test_get_to_time():
    assert parameters.get_to_time(None) == "now"
    assert parameters.get_to_time({"timeFilters": {"to": "42"}}) == "42"
    assert parameters.get_to_time({"timeFilters": {"foo": "bar"}}) == "now"

def test_get_dsl_filter():
    assert parameters.get_dsl_filter(None) is None
    assert parameters.get_dsl_filter({"filters": {}}) is None

def test_get_query():
    query = parameters.get_query({"query": {"language": "lucene", "query": "foo"}})
    assert query.get("lucene_query") == "foo"

    query = parameters.get_query({"query": {"language": "kuery", "query": "foo"}})
    assert query.get("lucene_query") == "foo"

    query = parameters.get_query({"query": {"language": "dsl", "query": "foo"}})
    assert query.get("dsl_query") == "foo"

    assert len(parameters.get_query({"query": {"query": "foo"}})) == 0

def test_get_render_mode():
    params = {
        "ellipses": "true",
        "ellipse_major": "42",
        "ellipse_minor": "52",
        "ellipse_tilt": "62",
    }

    assert parameters.get_render_mode(params) == "ellipses"
    assert parameters.get_render_mode({"foo": "bar"}) == "points"
    assert parameters.get_render_mode({"render_mode": "foo"}) == "foo"

def test_get_ellipse_params():
    param_names = ("ellipse_major", "ellipse_minor", "ellipse_tilt", "ellipse_units")
    params = parameters.get_ellipse_params("ellipses", {name: "42" for name in param_names})

    for name in param_names:
        assert params.get(name) == "42"

    assert len(parameters.get_ellipse_params("points", params)) == 0

def test_get_search_distance():
    assert parameters.get_search_distance({"track_search": "narrow"}) == 1.0
    assert parameters.get_search_distance({"track_search": "normal"}) == 10.0
    assert parameters.get_search_distance({"track_search": "wide"}) == 50.0

    assert parameters.get_search_distance({"ellipse_search": "narrow"}) == 1.0
    assert parameters.get_search_distance({"ellipse_search": "normal"}) == 10.0
    assert parameters.get_search_distance({"ellipse_search": "wide"}) == 50.0

    assert parameters.get_search_distance({"track_search": "normal", "ellipse_search": "wide"}) == 10.0
    assert parameters.get_search_distance({}) == 50.0

def test_get_filter_distance():
    assert parameters.get_filter_distance(None) is None
    assert parameters.get_filter_distance("none") == 0.0
    assert parameters.get_filter_distance("short") == 1.0
    assert parameters.get_filter_distance("normal") == 10.0
    assert parameters.get_filter_distance("long") == 50.0
    assert parameters.get_filter_distance("42") == 42.0
    assert parameters.get_filter_distance("banana") is None

def test_get_category_histogram():
    assert parameters.get_category_histogram("TRUE") == True
    assert parameters.get_category_histogram("False") == False
    assert parameters.get_category_histogram("foo") is None

def test_get_cmap():
    assert parameters.get_cmap(None, None) == "bmy"
    assert parameters.get_cmap(None, "some_field") == "glasbey_category10"
    assert parameters.get_cmap("my_cmap", "some_field") == "my_cmap"
    assert parameters.get_cmap("my_cmap", None) == "my_cmap"
    assert parameters.get_cmap("", "") == "bmy"
    assert parameters.get_cmap("", "some_field") == "glasbey_category10"

def test_get_category_field():
    assert parameters.get_category_field("null") is None
    assert parameters.get_category_field(None) is None
    assert parameters.get_category_field("banana") == "banana"

def test_get_parameter_hash():
    assert parameters.get_parameter_hash({"foo": "bar", "baz": 1}) == "0d922e38f1a94fdc8acc6016c333d49e"
    assert parameters.get_parameter_hash({"foo": "bar", "baz": 1, "abc": datetime(2022, 2, 17, 11, 00, 00, tzinfo=timezone.utc)}) == "5463c0e1ae7f3c9e182afc2786050765"
    assert parameters.get_parameter_hash({}) == "d41d8cd98f00b204e9800998ecf8427e"
