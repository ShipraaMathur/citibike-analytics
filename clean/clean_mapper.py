#!/usr/bin/env python3
"""Stage 1 mapper: validate raw Citi Bike rows and derive the borough.

Output (key<TAB>value) is keyed by ride_id so the reducer can drop duplicate
trips. Each rejected row is counted under a reason in the Hadoop counter group
"Citibike" and is not emitted.

Kept record columns (after the ride_id key):
  rideable_type, started_at, ended_at, duration_sec, start_lat, start_lng,
  borough, member_casual
"""
import sys

from boroughs import borough_of
from cb import (IDX, NCOLS, LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX,
                MAX_DURATION_SEC, MIN_DURATION_SEC, STUDY_YEARS, Counters,
                is_header, norm_ts, parse_coord, parse_ts, split_line)

counters = Counters("Citibike")


def main():
    inc = counters.inc
    for line in sys.stdin:
        if not line.strip():
            inc("rejected_blank_line")
            continue
        if is_header(line):
            inc("skipped_header_line")
            continue
        inc("rows_read")
        fields = split_line(line)
        if fields is None or len(fields) != NCOLS:
            inc("rejected_bad_column_count")
            continue

        ride_id = fields[IDX["ride_id"]].strip()
        if not ride_id:
            inc("rejected_missing_ride_id")
            continue

        start = parse_ts(fields[IDX["started_at"]])
        if start is None:
            inc("rejected_bad_started_at")
            continue
        end = parse_ts(fields[IDX["ended_at"]])
        if end is None:
            inc("rejected_bad_ended_at")
            continue

        started = norm_ts(fields[IDX["started_at"]])
        if int(started[:4]) not in STUDY_YEARS:
            inc("rejected_outside_2023_2024")
            continue

        seconds = (end[0] - start[0]) * 86400 + (end[1] - start[1])
        if seconds < 0:
            inc("rejected_end_before_start")
            continue
        if seconds < MIN_DURATION_SEC:
            inc("rejected_duration_under_60s")
            continue
        if seconds > MAX_DURATION_SEC:
            inc("rejected_duration_over_24h")
            continue

        lat_raw = fields[IDX["start_lat"]].strip()
        lng_raw = fields[IDX["start_lng"]].strip()
        if not lat_raw or not lng_raw:
            inc("rejected_missing_start_coords")
            continue
        lat = parse_coord(lat_raw, LAT_MIN, LAT_MAX)
        lng = parse_coord(lng_raw, LNG_MIN, LNG_MAX)
        if lat is None or lng is None:
            inc("rejected_start_coords_outside_box")
            continue

        member = fields[IDX["member_casual"]].strip().lower()
        if member not in ("member", "casual"):
            inc("rejected_bad_member_casual")
            continue
        bike = fields[IDX["rideable_type"]].strip().lower() or "unknown"

        borough = borough_of(lat, lng)
        inc("kept")
        inc("kept_borough_" + borough.replace(" ", "_").replace("-", "_"))
        sys.stdout.write("%s\t%s\t%s\t%s\t%d\t%.6f\t%.6f\t%s\t%s\n" % (
            ride_id, bike, started, norm_ts(fields[IDX["ended_at"]]),
            seconds, lat, lng, borough, member))
    counters.flush()


if __name__ == "__main__":
    main()
