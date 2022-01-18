from datetime import datetime
from dateutil.tz import tzutc
import pytest
from elastic_datashader.helpers import timeutil


@pytest.mark.parametrize(
    "start,stop",
    (
        (None, datetime(2020, 5, 1, 0, 0, 5)),
        (datetime(2020, 5, 1, 0, 0, 5), None),
        (None, None),
    )
)
def test_quantize_time_range_no_stop(start, stop):
    with pytest.raises(ValueError):
        timeutil.quantize_time_range(start, stop)


@pytest.mark.parametrize(
    "start,stop,expected",
    (
        (
            datetime(2020, 5, 1, 0, 0, 5),
            datetime(2020, 5, 11, 12, 0, 1),
            (datetime(2020, 5, 1, 0, 0), datetime(2020, 5, 11, 12, 0)),
        ),
        (
            datetime(2020, 3, 1, 0, 0, 5),
            datetime(2020, 5, 11, 12, 0, 1),
            (datetime(2020, 3, 1, 0, 0), datetime(2020, 5, 11, 12, 0)),
        ),
        (
            datetime(2020, 5, 11, 12, 0, 0),
            datetime(2020, 5, 11, 12, 0, 3),
            (datetime(2020, 5, 11, 12, 0), datetime(2020, 5, 11, 12, 0, 3)),
        ),
    ),
)
def test_quantize_time_range(start, stop, expected):
    assert expected == timeutil.quantize_time_range(start, stop)


@pytest.mark.parametrize(
    "time_string,current_time,round_direction,expected",
    (
        ("now-3d", datetime(2020, 5, 11, 12), "down", datetime(2020, 5, 8, 12, tzinfo=tzutc())),
        (
            "now-3d",
            datetime(2020, 5, 11, 12),
            "down",
            datetime(2020, 5, 8, 12, tzinfo=tzutc())
        ),
        (
            "now+3d",
            datetime(2020, 5, 11, 12),            
            "down",
            datetime(2020, 5, 14, 12, tzinfo=tzutc())
        ),
        (
            "now-1d/d",
            datetime(2020, 5, 11, 12, 4, 20, 0),
            "down",
            datetime(2020, 5, 10, tzinfo=tzutc())
        ),
        (
            "now-1d/d",
            datetime(2020, 5, 11, 12, 4, 20, 0),
            "up",
            datetime(2020, 5, 10, 23, 59, 59, 999999, tzinfo=tzutc())
        ),
    ),
)
def test_convert_kibana_time(time_string, current_time, round_direction, expected):
    assert expected == timeutil.convert_kibana_time(time_string, current_time, round_direction)


@pytest.mark.parametrize(
    "seconds,expected",
    (
        (3601, "1h0m1s"),
        (-3601, "-1h0m1s"),
        (86404, "1d0h0m4s"),
        (61, "1m1s"),
        (59, "59s"),
    ),
)
def test_pretty_time_delta(seconds, expected):
    assert expected == timeutil.pretty_time_delta(seconds)
