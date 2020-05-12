#!/usr/bin/env python3
import calendar
from datetime import datetime, timedelta
from typing import Optional


UNITS = ["ms", "s", "m", "h", "d", "w", "M", "y"]


def parse_date(time_string: str) -> Optional[datetime]:
    """Attempt to parse ``time_string`` as ``datetime.datetime``

    :param time_string: String to attempt to convert to ``datetime`` object
    :return: Datetime object if parseable; otherwise, None

    :Examples:
    >>> parse_date("2000-05-02T00:00:01")
    datetime.datetime(2000, 5, 2, 0, 0, 1)
    >>> parse_date("2000-05-02T00:00:01Z")
    datetime.datetime(2000, 5, 2, 0, 0, 1)
    >>> parse_date("hello, world!") is None
    True
    """
    if time_string.endswith("Z"):
        time_string = time_string[:-1]
    try:
        return datetime.fromisoformat(time_string)
    except ValueError:
        return None


def compute_date(
    date_time: datetime,
    operation: str,
    num: float,
    unit: str
) -> datetime:
    """Perform date math

    :param date_time: Datetime object reference point
    :param operation: "+" or "-"
    :param num: Number of ``units`` to perform ``operation``
    :param unit: Units (milliseconds to years available)
    :return: ``date_time`` offset by ``num`` ``unit``
    :raises ValueError: if an unsupported unit or operation is provided

    :Example:
    >>> d = datetime(2020, 5, 1)
    >>> compute_date(d, "+", 3, "d")
    datetime.datetime(2020, 5, 4, 0, 0)
    >>> compute_date(d, "-", 3, "d")
    datetime.datetime(2020, 4, 28, 0, 0)
    """
    if unit == "ms":
        num *= 1000
        td = timedelta(microseconds=num)
    elif unit == "s":
        td = timedelta(seconds=num)
    elif unit == "m":
        td = timedelta(minutes=num)
    elif unit == "h" or unit == "H":
        td = timedelta(hours=num)
    elif unit == "d":
        td = timedelta(days=num)
    elif unit == "w":
        td = timedelta(weeks=num)
    elif unit == "M":
        td = timedelta(days=num * 30)
    elif unit == "y":
        td = timedelta(days=num * 365)
    else:
        raise ValueError(f"Unit '{unit}' not supported")

    if operation == "+":
        return date_time + td
    elif operation == "-":
        return date_time - td
    else:
        raise ValueError(f"Operation '{operation}' not supported")


def round_time(date_time: datetime, unit: str, up: bool = False) -> datetime:
    """Round datetime object to provided unit

    :param date_time: Datetime object to round
    :param unit: Unit to round to
    :param up: Whether to round up or down (defaults to False)
    :return: Rounded datetime object

    :Examples:
    >>>
    """
    # isoweekday() => Sunday as start of week; weekday() => Monday as start of week
    beginning_of_week = date_time - timedelta(days=date_time.isoweekday() % 7)
    end_of_week = beginning_of_week + timedelta(days=6)
    end_of_month = calendar.monthlen(date_time.year, date_time.month)
    if unit == "ms":
        new_ms = int(date_time.microsecond / 1000) * 1000
        if up:
            new_ms += 999
        return date_time.replace(microsecond=new_ms)
    if unit == "s":
        if up:
            return date_time.replace(microsecond=999999)
        else:
            return date_time.replace(microsecond=0)
    if unit == "m":
        if up:
            return date_time.replace(second=59, microsecond=999999)
        else:
            return date_time.replace(second=0, microsecond=0)
    if unit == "h" or unit == "H":
        if up:
            return date_time.replace(minute=59, second=59, microsecond=999999)
        else:
            return date_time.replace(minute=0, second=0, microsecond=0)
    if unit == "d":
        if up:
            return date_time.replace(hour=23, minute=59, second=59, microsecond=999999)
        else:
            return date_time.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "w":
        if up:
            return date_time.replace(
                day=end_of_week.day,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999
            )
        else:
            return date_time.replace(
                day=beginning_of_week.day,
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
    if unit == "M":
        if up:
            return date_time.replace(
                day=end_of_month,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999
            )
        else:
            return date_time.replace(day=0, hour=0, minute=0, second=0, microsecond=0)
    if unit == "y":
        if up:
            return date_time.replace(
                month=12, day=31, hour=23, minute=59, second=59, microsecond=999999
            )
        else:
            return date_time.replace(
                month=1,
                day=0,
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )


def parse_date_math(
    math_string: str,
    current_time: datetime,
    round_up: bool = False
) -> datetime:
    """Attempt to parse Kibana DateMath syntax (e.g., "now-3d")

    :param math_string: Just the part of the string with the
                        operations, numbers, and units
    :param current_time:
    :param round_up:
    :return:

    :Examples:
    >>> time_string = "-3d"
    >>> now = datetime(2020, 5, 1, 12, 10, 11)
    >>> parse_date_math(time_string, now)
    datetime.datetime(2020, 4, 28, 12, 10, 11)
    >>> parse_date_math(time_string, now, round_up=True)
    datetime.datetime(2020, 4, 28, 12, 10, 11)
    >>> time_string = "+3d"
    >>> parse_date_math(time_string, now)
    ...
    """
    date_time = current_time
    i = 0
    math_string_len = len(math_string)
    while i < math_string_len:
        char = math_string[i]
        i += 1
        if char == "/":
            type_ = 0
        elif char == "+":
            type_ = 1
        elif char == "-":
            type_ = 2
        else:
            raise ValueError(f"Invalid date math operator '{char}'")

        char = math_string[i]
        if not char.isdigit():
            num = 1
        elif len(math_string) == 2:
            num = math_string[i]
        else:
            num_from = i
            while math_string[i].isdigit():
                i += 1
                if i >= math_string_len:
                    raise ValueError("No unit provided")
            num = int(math_string[num_from:i], 10)

        # rounding is only allowed on whole, single,
        # units (eg M or 1M, not 0.5M or 2M)
        if type_ == 0 and num != 1:
            raise ValueError(
                "Rounding is only allowed on whole, single units"
                " (e.g., M or 1M, not 0.5M or 2M)."
            )

        unit = math_string[i]
        i += 1

        # append additional characters in the unit
        for j in range(i, math_string_len):
            unit_char = math_string[i]
            if unit_char.isalpha():
                unit += unit_char
                i += 1
            else:
                break

        if unit not in UNITS:
            raise ValueError(f"Unit {unit} is not a valid time offset")
        else:
            if type_ == 0:
                if round_up:  # End of ``unit``
                    date_time = round_time(date_time, unit, up=True)
                else:  # Start of ``unit``
                    date_time = round_time(date_time, unit, up=False)
            elif type_ == 1:  # add
                date_time = compute_date(date_time, "+", num, unit)
            elif type_ == 2:  # subtract
                date_time = compute_date(date_time, "-", num, unit)
    return date_time


def convert_kibana_time(
    time_string: str,
    current_time: datetime,
    round_up: bool = False
) -> datetime:
    """Convert Kibana date math to Python datetime object

    :param time_string: Kibana/ES/SOLR-style date math (e.g., now-3m)
    :param current_time: Basis for date math (this will be the ``now``)
    :param round_up: Whether or not to round up provided a /
    :return: Datetime object corresponding to ``time_string`` and ``current_time``
    :raises ValueError: if no ``time_string`` provided

    :Example:
    >>> now = datetime(2020, 5, 12, 15, 0, 0)
    >>> convert_kibana_time("now-3d", now)
    """
    if not time_string:
        raise ValueError("No time_string provided")

    if time_string[:3] == "now":
        math_string = time_string[3:]
    else:
        if "||" in time_string:
            parse_string, math_string = time_string.split("||")
        else:
            parse_string = time_string
            math_string = ""

        # only allowing ISO8601 as per Kibana
        current_time = datetime.fromisoformat(parse_string)

    if not math_string:
        return current_time

    return parse_date_math(math_string, current_time, round_up=round_up)
