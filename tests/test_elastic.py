#!/usr/bin/env pytest
import pytest
from tms_pixellock_api.helpers import elastic


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
