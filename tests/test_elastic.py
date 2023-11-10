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
    meta = {"disabled":False,"negate":False,"alias":None}
    # geo_distance with query key (built when you create a filter from the map)
    geo_distance = {"geo_distance":{"distance":"260km","point":[-83.89,34.7]}}
    q = {"bool":{"must":[{"exists":{"field":"point"}},{**geo_distance}]}}
    filters = [{"meta":{**meta,"type":"spatial_filter"},"query":{**q}}]
    expected = {'filter': [{'match_all': {}}, {**q}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # geo_distance filters without query
    filters = [{"meta":{**meta},**geo_distance}]
    expected = {'filter': [{'match_all': {}}, {**geo_distance}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # ensure disabled doesn't return a filter
    filters[0]['meta']['disabled'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # type phrase
    phrase = {"match_phrase":{"age":"10"}}
    filters = [{"meta":{**meta,"type":"phrase"},"query":{**phrase}}]
    expected = {'filter': [{'match_all': {}}, {**phrase}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate phrase
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**phrase}]}
    assert elastic.build_dsl_filter(filters) == expected

    # phrases
    q = {"bool":{"minimum_should_match":1,"should":[{**phrase},{"match_phrase":{"age":"11"}}]}}
    filters = [{"meta":{**meta,"type":"phrases"},"query":{**q}}]
    expected = {'filter': [{'match_all': {}}, {**q}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate phrases
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**q}]}
    assert elastic.build_dsl_filter(filters) == expected

    # Range filter
    rangeFilter = {"range":{"age":{"gte":"2","lt":"10"}}}
    filters = [{"meta":{**meta,"type":"range"},**rangeFilter}]
    expected = {'filter': [{'match_all': {}},{**rangeFilter}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate Range filter
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**rangeFilter}]}
    assert elastic.build_dsl_filter(filters) == expected

    # Range filter using query
    filters = [{"meta":{**meta,"type":"range"},"query":{**rangeFilter}}]
    expected = {'filter': [{'match_all': {}},{**rangeFilter}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate Range filter
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**rangeFilter}]}
    assert elastic.build_dsl_filter(filters) == expected

    # exists
    exists = {"exists":{"field": 'age'}}
    filters = [{"meta":{**meta,"type":"exists"},**exists}]
    expected = {'filter': [{'match_all': {}},{**exists}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate exists
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**exists}]}

    #exists using query
    filters = [{"meta":{**meta,"type":"exists"},"query":{**exists}}]
    expected = {'filter': [{'match_all': {}},{**exists}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate exists
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**exists}]}
    assert elastic.build_dsl_filter(filters) == expected

    # type custom spatial filter using key "query"
    q = {"bool":{"must":[{"exists":{"field":"point"}},{**geo_distance}]}}
    filters = [{"meta":{**meta,"type":"custom","key":"query"},"query":{**q}}]
    expected = {'filter': [{'match_all': {}}, {**q}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected

    # negate custom filter with key query
    filters[0]['meta']['negate'] = True
    expected = {'filter': [{'match_all': {}}], 'must_not': [{**q}]}
    assert elastic.build_dsl_filter(filters) == expected

    # filters from control dont send a type
    filters = [{"meta":{**meta},"query":{**phrase}}]
    expected = {'filter': [{'match_all': {}},{**phrase}], 'must_not': []}
    assert elastic.build_dsl_filter(filters) == expected


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

