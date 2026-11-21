#!/usr/bin/env python3
"""Create small synthetic Citi Bike zip files with known defects.

The zips imitate the real layout: a yearly 2023 zip with month folders and
split CSV parts, monthly 2024 zips, a nested zip, macOS junk entries and a
non-CSV file. expected.json records the exact counts the pipeline must find.

Usage: python3 make_synthetic.py <output_dir>
"""
import io
import json
import os
import random
import sys
import zipfile
from datetime import datetime, timedelta

HEADER = ("ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,"
          "end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual")

STATIONS = [
    ("W 55 St & 6 Ave", "6798.01", 40.7632, -73.9783, "Manhattan"),
    ("Broadway, & W 55 St", "6791.03", 40.7650, -73.9820, "Manhattan"),
    ("Grand Army Plaza & Plaza St", "3865.02", 40.6730, -73.9708, "Brooklyn"),
    ("Metropolitan Ave & Bedford Ave", "5438.04", 40.7145, -73.9603, "Brooklyn"),
    ("Vernon Blvd & 50 Ave", "6290.01", 40.7425, -73.9540, "Queens"),
    ("E 161 St & River Ave", "8036.02", 40.8279, -73.9293, "Bronx"),
    ("Hoboken Terminal - Hudson St & Hudson Pl", "HB101", 40.7359, -74.0300, "Non-NYC"),
]


def ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") + ".%03d" % (dt.microsecond // 1000)


def main(out):
    random.seed(42)
    os.makedirs(out, exist_ok=True)
    seq = [0]
    expected = {
        "rows_read": 0, "kept_pre_dedup": 0, "valid_unique": 0, "duplicates_removed": 0,
        "rejected_bad_column_count": 0, "rejected_missing_ride_id": 0,
        "rejected_bad_started_at": 0, "rejected_bad_ended_at": 0,
        "rejected_outside_2023_2024": 0, "rejected_end_before_start": 0,
        "rejected_duration_under_60s": 0, "rejected_duration_over_24h": 0,
        "rejected_missing_start_coords": 0, "rejected_start_coords_outside_box": 0,
        "rejected_bad_member_casual": 0, "skipped_header_line": 0,
        "borough_hour_trips": {},
    }

    def new_id():
        seq[0] += 1
        return "%016X" % (0xABC000000000 + seq[0] * 7919)

    def row(rid, start, dur, st, member="member", bike="classic_bike", **over):
        end = start + timedelta(seconds=dur)
        f = [rid, bike, ts(start), ts(end), st[0], st[1], "Some St & Other Ave", "1234.05",
             "%.6f" % st[2], "%.6f" % st[3], "40.750000", "-73.980000", member]
        for k, v in over.items():
            f[{"started": 2, "ended": 3, "lat": 8, "lng": 9, "id": 0, "member": 12}[k]] = v
        out_f = []
        for v in f:
            out_f.append('"%s"' % v if "," in v else v)
        return ",".join(out_f)

    valid_rows = {}

    def valid(start_lo, start_hi, n, rows, tag=None):
        for _ in range(n):
            start = start_lo + timedelta(seconds=random.randint(0, int((start_hi - start_lo).total_seconds())))
            dur = random.randint(90, 3000)
            st = random.choice(STATIONS)
            member = random.choice(["member", "casual"])
            bike = random.choice(["classic_bike", "electric_bike"])
            rid = new_id()
            line = row(rid, start, dur, st, member, bike)
            rows.append(line)
            valid_rows.setdefault(tag, []).append(line)
            expected["rows_read"] += 1
            expected["kept_pre_dedup"] += 1
            expected["valid_unique"] += 1
            key = "%s|%s|%02d" % (st[4], start.strftime("%Y-%m-%d"), start.hour)
            expected["borough_hour_trips"][key] = expected["borough_hour_trips"].get(key, 0) + 1

    def defects(year, rows, first_part):
        base = datetime(year, 1, 10, 12, 0, 0)
        st = STATIONS[0]

        def bad(counter, line):
            rows.append(line)
            expected["rows_read"] += 1
            expected[counter] += 1

        if first_part:
            bad("rejected_missing_ride_id", row("", base, 600, st, id=""))
            bad("rejected_bad_started_at", row(new_id(), base, 600, st, started="garbage"))
            bad("rejected_bad_started_at", row(new_id(), base, 600, st, started="2023-13-45 99:00:00.000"))
            bad("rejected_bad_ended_at", row(new_id(), base, 600, st, ended="not a time"))
            bad("rejected_end_before_start", row(new_id(), base, -500, st))
            bad("rejected_duration_under_60s", row(new_id(), base, 30, st))
            bad("rejected_duration_under_60s", row(new_id(), base, 59, st))
            bad("rejected_duration_over_24h", row(new_id(), base, 90000, st))
            bad("rejected_missing_start_coords", row(new_id(), base, 600, st, lat="", lng=""))
            bad("rejected_start_coords_outside_box", row(new_id(), base, 600, st, lat="0.000000", lng="0.000000"))
            bad("rejected_bad_member_casual", row(new_id(), base, 600, st, member="Subscriber"))
            bad("rejected_outside_2023_2024", row(new_id(), datetime(2022, 12, 31, 23, 40, 0), 600, st))
            bad("rejected_outside_2023_2024", row(new_id(), datetime(2025, 1, 1, 0, 5, 0), 600, st))
            bad("rejected_bad_column_count", "short,row,only,five,fields")
            bad("rejected_bad_column_count", row(new_id(), base, 600, st) + ",extra")
            # a repeated header line in the middle of the file
            rows.append(HEADER)
            expected["skipped_header_line"] += 1

    def csv_bytes(rows):
        return ("\n".join([HEADER] + rows) + "\n").encode()

    jan23 = (datetime(2023, 1, 1), datetime(2023, 1, 31, 23, 59, 59))
    feb23 = (datetime(2023, 2, 1), datetime(2023, 2, 28, 23, 59, 59))
    jan24 = (datetime(2024, 1, 1), datetime(2024, 1, 31, 23, 59, 59))
    feb24 = (datetime(2024, 2, 1), datetime(2024, 2, 28, 23, 59, 59))

    expected["skipped_header_line"] = 0

    part = {}
    for name, rng, n, year, first in [
        ("202301_1", jan23, 120, 2023, True), ("202301_2", jan23, 90, 2023, False),
        ("202302_1", feb23, 100, 2023, False),
        ("202401_1", jan24, 110, 2024, True), ("202402_1", feb24, 80, 2024, False)]:
        rows = []
        valid(rng[0], rng[1], n, rows, tag=name)
        defects(year, rows, first)
        part[name] = rows
        # the file's own header line is counted too (it is a header)
        expected["skipped_header_line"] += 1

    # exact duplicates of five valid rows from 202301_1, placed in 202301_2
    picked = valid_rows["202301_1"][:5]
    part["202301_2"].extend(picked)
    expected["rows_read"] += 5
    expected["kept_pre_dedup"] += 5
    expected["duplicates_removed"] = 5

    # raw data-line count the profile job should see as well-formed (13 columns)
    well_formed = expected["rows_read"] - expected["rejected_bad_column_count"]
    expected["profile_rows_total"] = well_formed
    expected["profile_rows_bad_column_count"] = expected["rejected_bad_column_count"]

    def write_zip(path, members):
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
            for name, data in members:
                z.writestr(name, data)

    write_zip(os.path.join(out, "2023-citibike-tripdata.zip"), [
        ("2023-citibike-tripdata/1_January/202301-citibike-tripdata_1.csv", csv_bytes(part["202301_1"])),
        ("2023-citibike-tripdata/1_January/202301-citibike-tripdata_2.csv", csv_bytes(part["202301_2"])),
        ("2023-citibike-tripdata/2_February/202302-citibike-tripdata_1.csv", csv_bytes(part["202302_1"])),
        ("__MACOSX/2023-citibike-tripdata/1_January/._202301-citibike-tripdata_1.csv", b"junk"),
        ("2023-citibike-tripdata/readme.txt", b"hello"),
    ])
    write_zip(os.path.join(out, "202401-citibike-tripdata.zip"), [
        ("202401-citibike-tripdata_1.csv", csv_bytes(part["202401_1"])),
        ("__MACOSX/._202401-citibike-tripdata_1.csv", b"junk"),
    ])
    inner = io.BytesIO()
    write_zip(inner, [("202402-citibike-tripdata_1.csv", csv_bytes(part["202402_1"]))])
    write_zip(os.path.join(out, "202402-citibike-tripdata.zip"),
              [("202402-citibike-tripdata_inner.zip", inner.getvalue())])

    json.dump(expected, open(os.path.join(out, "expected.json"), "w"), indent=1, sort_keys=True)
    print("wrote synthetic zips to %s" % out)
    print("valid unique trips: %d, total data rows: %d" % (expected["valid_unique"], expected["rows_read"]))


if __name__ == "__main__":
    main(sys.argv[1])
