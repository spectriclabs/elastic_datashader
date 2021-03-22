#!/usr/bin/env pytest
import pytest
from tms_datashader_api.helpers import elastic


def test_get_search_base():
    pass


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