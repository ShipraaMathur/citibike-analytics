#!/usr/bin/env python3
"""Stage 1 reducer: keep one record per ride_id.

Input is sorted by ride_id. When the same ride_id appears more than once, the
lexicographically smallest record is kept so the result does not depend on the
order Hadoop delivers values in. Dropped copies are counted as
Citibike,duplicates_removed.
"""
import sys

from cb import Counters

counters = Counters("Citibike")


def main():
    current = None
    best = None
    for line in sys.stdin:
        line = line.rstrip("\n")
        if "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        if key != current:
            if current is not None:
                sys.stdout.write("%s\t%s\n" % (current, best))
                counters.inc("kept_after_dedup")
            current, best = key, value
        else:
            counters.inc("duplicates_removed")
            if value < best:
                best = value
    if current is not None:
        sys.stdout.write("%s\t%s\n" % (current, best))
        counters.inc("kept_after_dedup")
    counters.flush()


if __name__ == "__main__":
    main()
