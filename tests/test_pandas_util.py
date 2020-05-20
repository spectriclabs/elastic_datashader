#!/usr/bin/env pytest
import pandas as pd
import pytest
from tms_datashader_api.helpers import pandas_util


def test_simplify_categories_not_categorical():
    df = pd.DataFrame({"foo": [1, 2, 2, 3, 4, 4]})
    with pytest.raises(ValueError):
        pandas_util.simplify_categories(df, "foo", {})


def test_simplify_categories_missing_colors():
    df = pd.DataFrame({"foo": ["1", "2", "2", "3", "4", "4"]})
    df["foo"] = df.foo.astype("category")
    with pytest.raises(ValueError):
        pandas_util.simplify_categories(df, "foo", {"1": "#ff0000", "2": "#00ff00"})


def test_simplify_categories_color_key_wrong_type():
    df = pd.DataFrame({"foo": [1, 2, 2, 3, 4, 4]})
    df["foo"] = df.foo.astype("category")
    with pytest.raises(TypeError):
        pandas_util.simplify_categories(df, "foo", "bar")


def test_simplify_categories_list():
    df = pd.DataFrame({"foo": [1, 2, 2, 3, 4, 4]})
    df["foo"] = df.foo.astype("category")
    actual_df, actual_ck = pandas_util.simplify_categories(
        df, "foo", ["#ff0000", "#00ff00", "#0000ff", "#ffffff"]
    )
    expected_ck = {
        "#ff0000": "#ff0000",
        "#00ff00": "#00ff00",
        "#0000ff": "#0000ff",
        "#ffffff": "#ffffff",
    }
    assert expected_ck == actual_ck

    expected_df = pd.DataFrame(
        {"foo": ["#ff0000", "#00ff00", "#00ff00", "#0000ff", "#ffffff", "#ffffff"]}
    )
    expected_df["foo"] = expected_df.foo.astype("category")
    assert expected_df.equals(actual_df)


@pytest.mark.parametrize(
    "threshold,last,exception", ((None, None, ValueError), (5, 10, ValueError))
)
def test_replace_low_freq_inplace_errors(threshold, last, exception):
    s = pd.Series([3, 1, 2, 3, 4]).astype("category")
    with pytest.raises(exception):
        pandas_util.replace_low_freq_inplace(s, threshold=threshold, last=last)


@pytest.mark.parametrize(
    "threshold,last,expected",
    (
        (2, None, pd.Series([3, "Other", "Other", 3, "Other"]).astype("category")),
        (None, 2, pd.Series([3, "Other", "Other", 3, 4]).astype("category")),
    ),
)
def test_replace_low_freq_inplace(threshold, last, expected):
    s = pd.Series([3, 1, 2, 3, 4]).astype("category")
    pandas_util.replace_low_freq_inplace(s, threshold=threshold, last=last)
    assert expected.equals(s)
