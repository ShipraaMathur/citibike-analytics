#!/usr/bin/env python3
"""Profiling mapper for raw Citi Bike CSV rows (Hadoop Streaming).

Emits "metric<TAB>value" lines. Metrics are accumulated inside the mapper and
written once at the end, so the shuffle carries a few hundred lines per task
instead of one line per trip.

Metric prefixes understood by profile_reducer.py:
  min:<name> and max:<name>  keep the smallest/largest string
  anything else              is summed
"""
import sys

from cb import (COLUMNS, IDX, NCOLS, LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX,
                is_header, parse_coord, parse_ts, split_line)

counts = {}
mins = {}
maxs = {}


def inc(key, n=1):
    counts[key] = counts.get(key, 0) + n


def track_range(name, value):
    if name not in mins or value < mins[name]:
        mins[name] = value
    if name not in maxs or value > maxs[name]:
        maxs[name] = value


def duration_bucket(seconds):
    if seconds < 0:
        return "00_negative"
    if seconds < 60:
        return "01_under_1_min"
    if seconds < 300:
        return "02_1_to_5_min"
    if seconds < 900:
        return "03_5_to_15_min"
    if seconds < 1800:
        return "04_15_to_30_min"
    if seconds < 3600:
        return "05_30_to_60_min"
    if seconds < 86400:
        return "06_1_to_24_hours"
    return "07_over_24_hours"


def main():
    for line in sys.stdin:
        if not line.strip():
            inc("rows_blank")
            continue
        if is_header(line):
            inc("rows_header")
            continue
        fields = split_line(line)
        if fields is None or len(fields) != NCOLS:
            inc("rows_bad_column_count")
            continue
        inc("rows_total")

        for i, name in enumerate(COLUMNS):
            if fields[i].strip() == "":
                inc("null:" + name)

        inc("rideable_type:" + (fields[IDX["rideable_type"]].strip() or "(empty)"))
        inc("member_casual:" + (fields[IDX["member_casual"]].strip() or "(empty)"))

        start = parse_ts(fields[IDX["started_at"]])
        end = parse_ts(fields[IDX["ended_at"]])
        if start is None:
            inc("invalid_started_at")
        else:
            started = fields[IDX["started_at"]].strip()
            inc("started_month:" + started[:7])
            inc("started_hour:" + started[11:13])
            track_range("started_at", started[:19])
        if end is None:
            inc("invalid_ended_at")
        if start is not None and end is not None:
            seconds = (end[0] - start[0]) * 86400 + (end[1] - start[1])
            inc("duration:" + duration_bucket(seconds))

        lat = parse_coord(fields[IDX["start_lat"]], LAT_MIN, LAT_MAX)
        lng = parse_coord(fields[IDX["start_lng"]], LNG_MIN, LNG_MAX)
        if fields[IDX["start_lat"]].strip() == "" or fields[IDX["start_lng"]].strip() == "":
            pass  # already counted as a null column
        elif lat is None or lng is None:
            inc("start_coord_invalid_or_outside_box")

    for key, n in counts.items():
        sys.stdout.write("%s\t%d\n" % (key, n))
    for key, value in mins.items():
        sys.stdout.write("min:%s\t%s\n" % (key, value))
    for key, value in maxs.items():
        sys.stdout.write("max:%s\t%s\n" % (key, value))


if __name__ == "__main__":
    main()
