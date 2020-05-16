#!/usr/bin/env python3
import math
from datetime import timedelta, datetime
from typing import Optional, Tuple

import arrow
import datemath


def quantize_time_range(
    start_time: Optional[datetime], stop_time: datetime
) -> Tuple[Optional[datetime], datetime]:
    """Quantize the start and end times so when Kibana uses
    "now" we do not constantly invalidate cache.

    :param start_time: Start time
    :param stop_time: Stop time
    :return: Quantized start and end times

    :Example:
    >>> start = datetime(2020, 5, 1, 0, 0, 5)
    >>> end = datetime(2020, 5, 11, 12, 0, 1)
    >>> quantize_time_range(None, end)
    (None, datetime.datetime(2020, 5, 11, 0, 0))
    >>> qstart, qend = quantize_time_range(start, end)
    >>> qstart
    datetime.datetime(2020, 5, 1, 0, 0)
    >>> qend
    datetime.datetime(2020, 5, 11, 0, 0)
    """
    if stop_time is None:
        raise ValueError("stop time must be provided")

    # If the range is all time, just truncate to rayday
    if start_time is None:
        stop_time = stop_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_time, stop_time

    # Calculate the span
    delta_time = stop_time - start_time

    if delta_time > timedelta(days=29):
        # delta > 29 days, truncate to rayday
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        stop_time = stop_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_time, stop_time
    elif delta_time > timedelta(days=1):
        # More than a day, truncate to an hour
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        stop_time = stop_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_time, stop_time
    else:
        # truncate to 5 min
        start_time = start_time.replace(
            minute=math.floor(start_time.minute / 5.0) * 5, second=0, microsecond=0
        )
        stop_time = stop_time.replace(
            minute=math.floor(stop_time.minute / 5.0) * 5, second=0, microsecond=0
        )
        return start_time, stop_time


def convert_kibana_time(time_string, current_time):
    """Convert Kibana/ES date math into Python datetimes

    :param time_string: Time-string following
    :param current_time: Reference point for date math
    :return: Datetime object based on ``time_string`` math

    :Examples:
    >>> now = datetime(2020, 5, 12, 15, 0, 0)
    >>> convert_kibana_time("now-3m", now)
    datetime.datetime(2020, 5, 12, 14, 57, tzinfo=tzutc())
    """
    if isinstance(current_time, datetime):
        current_time = arrow.get(current_time)
    return datemath.datemath(time_string, now=current_time)


def pretty_time_delta(seconds: int) -> str:
    """Format seconds timedelta to days, hours, minutes, seconds

    :param seconds: Seconds representing a timedelta
    :return: Formatted timedelta

    :Example:
    >>> pretty_time_delta(3601)
    '1h0m1s'
    >>> pretty_time_delta(-3601)
    '-1h0m1s'
    """
    sign_string = "-" if seconds < 0 else ""
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return "%s%dd%dh%dm%ds" % (sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return "%s%dh%dm%ds" % (sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return "%s%dm%ds" % (sign_string, minutes, seconds)
    else:
        return "%s%ds" % (sign_string, seconds)
