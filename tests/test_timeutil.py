#!/usr/bin/env pytest
from datetime import datetime
from dateutil.tz import tzutc
import pytest
from tms_pixellock_api.helpers import timeutil


def test_quantize_time_range_no_stop():
    with pytest.raises(ValueError):
        timeutil.quantize_time_range(None, None)


@pytest.mark.parametrize(
    "start,stop,expected",
    (
        (
            None,
            datetime(2020, 5, 11, 12, 0, 1),
            (None, datetime(2020, 5, 11, 0, 0)),
        ),
        (
            datetime(2020, 5, 1, 0, 0, 5),
            datetime(2020, 5, 11, 12, 0, 1),
            (datetime(2020, 5, 1, 0, 0), datetime(2020, 5, 11, 0, 0)),
        ),
        (
            datetime(2020, 3, 1, 0, 0, 5),
            datetime(2020, 5, 11, 12, 0, 1),
            (datetime(2020, 3, 1, 0, 0), datetime(2020, 5, 11, 0, 0)),
        ),
        (
            datetime(2020, 5, 11, 12, 0, 0),
            datetime(2020, 5, 11, 12, 0, 3),
            (datetime(2020, 5, 11, 12, 0), datetime(2020, 5, 11, 12, 0)),
        ),
    ),
)
def test_quantize_time_range(start, stop, expected):
    assert expected == timeutil.quantize_time_range(start, stop)


@pytest.mark.parametrize(
    "time_string,current_time,expected",
    (
        ("now-3d", datetime(2020, 5, 11, 12), datetime(2020, 5, 8, 12, tzinfo=tzutc())),
        ("now+3d", datetime(2020, 5, 11, 12), datetime(2020, 5, 14, 12, tzinfo=tzutc())),
    )
)
def test_convert_kibana_time(time_string, current_time, expected):
    assert expected == timeutil.convert_kibana_time(time_string, current_time)


@pytest.mark.parametrize(
    "seconds,expected",
    (
        (3601, "1h0m1s"),
        (-3601, "-1h0m1s"),
        (86404, "1d0h0m4s"),
        (61, "1m1s"),
        (59, "59s"),
    )
)
def test_pretty_time_delta(seconds, expected):
    assert expected == timeutil.pretty_time_delta(seconds)
