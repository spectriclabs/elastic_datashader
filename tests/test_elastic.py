from datetime import datetime, timezone
import pytest

from elastic_datashader import elastic

def test_get_search_base():
    params = {
        "dsl_filter": None,
        "dsl_query": None,
        "lucene_query": None,
        "start_time": datetime(2022, 6, 15, 12, 30, 0, tzinfo=timezone.utc),
        "stop_time": datetime(2022, 6, 15, 12, 35, 0, tzinfo=timezone.utc),
        "timestamp_field": "footime",
    }

    base_s = elastic.get_search_base("http://localhost:9200", {}, params, "foo")
    base_dict = base_s.to_dict()
    assert base_dict.get("query") is not None
    assert base_dict.get("query").get("bool") is not None
    assert base_dict.get("query").get("bool").get("filter") is not None
    filters = base_dict.get("query", {}).get("bool", {}).get("filter", {})

    assert len(filters) > 0

    range_filter = filters[0].get("range")
    assert range_filter is not None

    assert params["timestamp_field"] in range_filter
    assert "gte" in range_filter[params["timestamp_field"]]
    assert "lte" in range_filter[params["timestamp_field"]]

    assert range_filter[params["timestamp_field"]]["gte"] == params["start_time"]
    assert range_filter[params["timestamp_field"]]["lte"] == params["stop_time"]

def test_build_dsl_filter():
    pass


def test_get_es_headers():
    pass


def test_convert():
    pass

@pytest.mark.parametrize(
    "field,expected",
    (
        ("foo.keyword", ["foo"]),
        ("bar", ["bar"]),
        ("blah.raw", ["blah"]),
    )
)
def test_split_fieldname_to_list(field, expected):
    assert expected == elastic.split_fieldname_to_list(field)

def test_get_nested_field_from_hit():
    pass

@pytest.mark.parametrize(
    "filter_input,filter_type,new_way,expected",
    (
        ({"meta": {"type": "exists"}, "query": {"exists": {"field": "foo"}}}, "exists", True, {"exists": {"field": "foo"}}),
        ({"meta": {"type": "range"}, "range": {"from": "foo", "to": "bar"}}, "range", False, {"range": {"from": "foo", "to": "bar"}}),
    )
)
def test_handle_range_or_exists_filters(filter_input, filter_type, new_way, expected):
    filter_output = elastic.handle_range_or_exists_filters(filter_input)
    assert len(filter_output) == 1
    assert filter_type in filter_output
    assert type(filter_output[filter_type]) is dict

    if new_way:
        expected_output = filter_input["query"]
    else:
        expected_output = {filter_type: filter_input[filter_type]}

    for key in expected_output:
        assert key in filter_output

        for subkey in expected_output[key]:
            assert subkey in filter_output[key]
            assert expected_output[key][subkey] == filter_output[key][subkey]

def test_chunk_iter():
    for has_more, chunk in elastic.chunk_iter([], 1000):
        assert True == False

    for has_more, chunk in elastic.chunk_iter(range(10), 1000):
        assert has_more ==  False
        assert len(chunk) == 10

    for has_more, chunk in elastic.chunk_iter(range(1000), 1000):
        assert has_more ==  False
        assert len(chunk) == 1000

    for ii, (has_more, chunk) in enumerate(elastic.chunk_iter(range(1001), 1000)):
        if ii == 0:
            assert has_more ==  True
            assert len(chunk) == 1000
        elif ii == 1:
            assert has_more ==  False
            assert len(chunk) == 1

    for ii, (has_more, chunk) in enumerate(elastic.chunk_iter(range(2000), 1000)):
        if ii == 0:
            assert has_more ==  True
            assert len(chunk) == 1000
        elif ii == 1:
            assert has_more ==  False
            assert len(chunk) == 1000

    for ii, (has_more, chunk) in enumerate(elastic.chunk_iter(range(2010), 1000)):
        if ii == 0:
            assert has_more ==  True
            assert len(chunk) == 1000
        elif ii == 1:
            assert has_more ==  True
            assert len(chunk) == 1000
        elif ii == 2:
            assert has_more ==  False
            assert len(chunk) == 10
