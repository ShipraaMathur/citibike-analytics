#!/usr/bin/env python3
"""Compare pipeline results with the counts recorded by make_synthetic.py.

Usage: python3 verify.py <expected.json> <project_dir> <fake_hdfs_root> <hdfs_base>
"""
import json
import os
import re
import sys

expected = json.load(open(sys.argv[1]))
proj, root, base = sys.argv[2], sys.argv[3], sys.argv[4]
failures = []


def check(label, got, want):
    ok = got == want
    print("%s  %-44s got=%s want=%s" % ("PASS" if ok else "FAIL", label, got, want))
    if not ok:
        failures.append(label)


# raw layout in HDFS
raw = os.path.join(root, base.lstrip("/"), "raw")
found = sorted(os.path.relpath(os.path.join(d, f), raw) for d, _, fs in os.walk(raw) for f in fs)
check("raw files loaded",
      found,
      ["2023/202301-citibike-tripdata_1.csv", "2023/202301-citibike-tripdata_2.csv",
       "2023/202302-citibike-tripdata_1.csv", "2024/202401-citibike-tripdata_1.csv",
       "2024/202402-citibike-tripdata_1.csv"])

# profile results
prof = dict(line.rstrip("\n").split("\t") for line in open(os.path.join(proj, "profile_results.tsv")))
check("profile rows_total", int(prof["rows_total"]), expected["profile_rows_total"])
check("profile rows_bad_column_count", int(prof["rows_bad_column_count"]),
      expected["profile_rows_bad_column_count"])
check("profile rows_header", int(prof["rows_header"]), expected["skipped_header_line"])
check("profile invalid_started_at", int(prof["invalid_started_at"]), expected["rejected_bad_started_at"])
check("profile null:ride_id", int(prof["null:ride_id"]), expected["rejected_missing_ride_id"])
check("profile min started_at is 2022 row", prof["min:started_at"][:4], "2022")
check("profile max started_at is 2025 row", prof["max:started_at"][:4], "2025")

# clean counters from the job log
log = open(os.path.join(proj, "logs", "clean_stage1.log")).read()
counters = {m.group(1): int(m.group(2))
            for m in re.finditer(r"^\s+([a-z0-9_]+)=(\d+)\s*$", log, re.M)}
check("rows_read", counters["rows_read"], expected["rows_read"])
for name in [k for k in expected if k.startswith("rejected_")]:
    check(name, counters.get(name, 0), expected[name])
check("skipped_header_line", counters["skipped_header_line"], expected["skipped_header_line"])
check("kept (before dedup)", counters["kept"], expected["kept_pre_dedup"])
check("duplicates_removed", counters["duplicates_removed"], expected["duplicates_removed"])
check("kept_after_dedup", counters["kept_after_dedup"], expected["valid_unique"])

# borough x hour table equals the expected hourly counts exactly
got = {}
total = 0
for line in open(os.path.join(proj, "borough_hour.tsv")):
    f = line.rstrip("\n").split("\t")
    assert len(f) == 14, f
    got["%s|%s|%02d" % (f[0], f[1], int(f[2]))] = int(f[7])
    total += int(f[7])
    assert int(f[7]) == int(f[8]) + int(f[9]) == int(f[10]) + int(f[11]) + int(f[12])
check("borough_hour cells", len(got), len(expected["borough_hour_trips"]))
check("borough_hour trips total", total, expected["valid_unique"])
check("borough_hour matches expected cell by cell", got == expected["borough_hour_trips"], True)

# trip-level output: one row per unique ride_id, 9 tab-separated columns
trips = []
tdir = os.path.join(root, base.lstrip("/"), "clean", "trips")
for n in sorted(os.listdir(tdir)):
    if n.startswith("part-"):
        trips += [l.rstrip("\n").split("\t") for l in open(os.path.join(tdir, n))]
check("trip rows", len(trips), expected["valid_unique"])
check("trip ride_ids unique", len({t[0] for t in trips}), expected["valid_unique"])
check("trip rows have 9 columns", all(len(t) == 9 for t in trips), True)

print()
if failures:
    print("%d check(s) failed: %s" % (len(failures), ", ".join(failures)))
    sys.exit(1)
print("All checks passed.")
