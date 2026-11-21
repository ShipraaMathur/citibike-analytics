"""Shared helpers for the Citi Bike MapReduce jobs (Hadoop Streaming, Python 3).

Citi Bike trip files from 2021 onward have 13 columns:
ride_id, rideable_type, started_at, ended_at, start_station_name,
start_station_id, end_station_name, end_station_id, start_lat, start_lng,
end_lat, end_lng, member_casual

Timestamps are New York local wall-clock time with no zone marker, for
example 2023-06-01 08:15:42.123. They are never converted here.
"""
import csv
import sys
from datetime import date

COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name",
    "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng",
    "member_casual",
]
NCOLS = len(COLUMNS)
IDX = {name: i for i, name in enumerate(COLUMNS)}

# Rough box around New York City plus the Jersey City and Hoboken stations.
LAT_MIN, LAT_MAX = 40.40, 41.00
LNG_MIN, LNG_MAX = -74.30, -73.60

STUDY_YEARS = (2023, 2024)
MIN_DURATION_SEC = 60        # Citi Bike treats shorter trips as false starts
MAX_DURATION_SEC = 86400     # longer than a day is almost always a lost bike


def split_line(line):
    """Split one CSV line. Returns a list of fields, or None if unparsable."""
    line = line.rstrip("\r\n")
    if '"' in line:
        try:
            return next(csv.reader([line]))
        except (csv.Error, StopIteration):
            return None
    return line.split(",")


def is_header(line):
    return line.startswith("ride_id,") or line.startswith('"ride_id"')


_date_cache = {}


def _ordinal(s10):
    """Ordinal day number for 'YYYY-MM-DD', or None if not a real date."""
    try:
        return _date_cache[s10]
    except KeyError:
        pass
    value = None
    if len(s10) == 10 and s10[4] == "-" and s10[7] == "-":
        try:
            value = date(int(s10[:4]), int(s10[5:7]), int(s10[8:10])).toordinal()
        except ValueError:
            value = None
    if len(_date_cache) > 5000:
        _date_cache.clear()
    _date_cache[s10] = value
    return value


def parse_ts(s):
    """Parse 'YYYY-MM-DD HH:MM:SS[.fff]' (a 'T' separator is accepted too).

    Returns (ordinal_day, seconds_of_day) or None when the value is invalid.
    """
    s = s.strip()
    if len(s) < 19 or s[10] not in " T" or s[13] != ":" or s[16] != ":":
        return None
    ordinal = _ordinal(s[:10])
    if ordinal is None:
        return None
    try:
        h, m, sec = int(s[11:13]), int(s[14:16]), int(s[17:19])
    except ValueError:
        return None
    if h > 23 or m > 59 or sec > 59:
        return None
    return ordinal, h * 3600 + m * 60 + sec


def norm_ts(s):
    """'2023-06-01T08:15:42.123' -> '2023-06-01 08:15:42'."""
    s = s.strip()
    return s[:10] + " " + s[11:19]


def parse_coord(value, lo, hi):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if x != x or not (lo <= x <= hi):
        return None
    return x


class Counters(object):
    """In-mapper counters flushed to Hadoop through stderr."""

    def __init__(self, group):
        self.group = group
        self.counts = {}

    def inc(self, name, n=1):
        self.counts[name] = self.counts.get(name, 0) + n

    def flush(self):
        for name, n in sorted(self.counts.items()):
            sys.stderr.write("reporter:counter:%s,%s,%d\n" % (self.group, name, n))
        sys.stderr.flush()
