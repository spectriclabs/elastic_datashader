from datetime import datetime
from typing import Tuple

import math

import arrow
import datemath

def quantize_time_range(
    start_time: datetime,
    stop_time: datetime
) -> Tuple[datetime, datetime]:
    """Quantize the start and end times to 5 min boundaries.

    :param start_time: Start time
    :param stop_time: Stop time
    :return: Quantized start and end times
    :raises ValueError: if ``start_time`` or ``stop_time`` is ``None``

    :Examples:
    >>> start = datetime(2020, 5, 1, 12, 0, 1)
    >>> stop = datetime(2020, 5, 1, 16, 0, 6)
    >>> qstart, qstop = quantize_time_range(start, stop)
    >>> qstart
    datetime.datetime(2020, 5, 1, 12, 0)
    >>> qstop
    datetime.datetime(2020, 5, 1, 16, 0)
    """
    if start_time is None or stop_time is None:
        raise ValueError("both start and stop times must be provided")

    # truncate to 5 min
    truncated_start_time = start_time.replace(
        minute=math.floor(start_time.minute / 5.0) * 5, second=0, microsecond=0
    )
    truncated_stop_time = stop_time.replace(
        minute=math.floor(stop_time.minute / 5.0) * 5, second=0, microsecond=0
    )
    if truncated_start_time == truncated_stop_time:
        return start_time, stop_time

    return truncated_start_time, truncated_stop_time


def convert_kibana_time(time_string: str, current_time: datetime, round_direction='down'):
    """
    Convert Kibana/ES date math into Python datetimes

    :param time_string: Time-string following
    :param current_time: Reference point for date math
    :param round_direction: Whether to round up or down (default: "down")
    :return: Datetime object based on ``time_string`` math

    :Examples:
    >>> now = datetime(2020, 5, 12, 15, 0, 0)
    >>> convert_kibana_time("now-3m", now)
    datetime.datetime(2020, 5, 12, 14, 57, tzinfo=tzutc())
    """
    current_time = arrow.get(current_time)
    result = arrow.get(datemath.datemath(time_string, now=current_time))

    if round_direction == 'up':
        # Kibana treats time rounding different than elasticsearch
        if time_string.endswith("/s"):
            result = result.shift(seconds=1, microseconds=-1)
        elif time_string.endswith("/m"):
            result = result.shift(minutes=1, microseconds=-1)
        elif time_string.endswith("/h"):
            result = result.shift(hours=1, microseconds=-1)
        elif time_string.endswith("/d"):
            result = result.shift(days=1, microseconds=-1)
        elif time_string.endswith("/w"):
            result = result.shift(weeks=1, microseconds=-1)
        elif time_string.endswith("/M"):
            result = result.shift(months=1, microseconds=-1)
        elif time_string.endswith("/y"):
            result = result.shift(years=1, microseconds=-1)

    return result.datetime


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

    if hours > 0:
        return "%s%dh%dm%ds" % (sign_string, hours, minutes, seconds)

    if minutes > 0:
        return "%s%dm%ds" % (sign_string, minutes, seconds)

    return "%s%ds" % (sign_string, seconds)
