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
