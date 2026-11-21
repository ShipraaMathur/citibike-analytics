#!/usr/bin/env python3
"""Stage 2 mapper: count cleaned trips per borough, local date and hour.

Reads the stage 1 output (9 tab-separated columns):
  ride_id, rideable_type, started_at, ended_at, duration_sec,
  start_lat, start_lng, borough, member_casual

Accumulates inside the mapper and writes one line per borough-hour at the end:
  borough|date|hour <TAB> trips,member,casual,electric,classic,other,duration_sum
"""
import sys

acc = {}


def main():
    for line in sys.stdin:
        f = line.rstrip("\n").split("\t")
        if len(f) != 9:
            continue
        started = f[2]
        key = "%s|%s|%s" % (f[7], started[:10], started[11:13])
        try:
            duration = int(f[4])
        except ValueError:
            continue
        row = acc.get(key)
        if row is None:
            row = acc[key] = [0, 0, 0, 0, 0, 0, 0]
        row[0] += 1
        if f[8] == "member":
            row[1] += 1
        else:
            row[2] += 1
        if f[1] == "electric_bike":
            row[3] += 1
        elif f[1] == "classic_bike":
            row[4] += 1
        else:
            row[5] += 1
        row[6] += duration
    for key, row in acc.items():
        sys.stdout.write("%s\t%s\n" % (key, ",".join(str(v) for v in row)))


if __name__ == "__main__":
    main()
