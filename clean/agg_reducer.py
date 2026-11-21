#!/usr/bin/env python3
"""Stage 2 reducer: final borough x hour table, tab-separated, Hive ready.

Output columns:
  borough, trip_date, trip_hour, year, month, day_of_week, is_weekend,
  trips, member_trips, casual_trips, electric_trips, classic_trips,
  other_type_trips, total_duration_sec

trip_date and trip_hour are New York local time exactly as Citi Bike
recorded them.
"""
import sys
from datetime import date

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def emit(key, totals):
    borough, day, hour = key.split("|")
    d = date(int(day[:4]), int(day[5:7]), int(day[8:10]))
    weekday = d.weekday()
    sys.stdout.write("\t".join([
        borough, day, str(int(hour)), str(d.year), str(d.month),
        DAYS[weekday], "1" if weekday >= 5 else "0",
    ] + [str(v) for v in totals]) + "\n")


def main():
    current = None
    totals = None
    for line in sys.stdin:
        line = line.rstrip("\n")
        if "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        try:
            parts = [int(v) for v in value.split(",")]
        except ValueError:
            continue
        if len(parts) != 7:
            continue
        if key != current:
            if current is not None:
                emit(current, totals)
            current, totals = key, [0] * 7
        for i in range(7):
            totals[i] += parts[i]
    if current is not None:
        emit(current, totals)


if __name__ == "__main__":
    main()
