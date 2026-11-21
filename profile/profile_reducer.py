#!/usr/bin/env python3
"""Reducer (and combiner) for the profiling job.

Input lines are "metric<TAB>value". Keys starting with min: or max: keep the
smallest or largest string value; every other key is summed. The output has
the same shape as the input, so the script is safe to use as a combiner.
"""
import sys


def main():
    current = None
    total = 0
    extreme = None
    for line in sys.stdin:
        line = line.rstrip("\n")
        if "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        if key != current:
            if current is not None:
                emit(current, total, extreme)
            current, total, extreme = key, 0, None
        if key.startswith("min:"):
            extreme = value if extreme is None or value < extreme else extreme
        elif key.startswith("max:"):
            extreme = value if extreme is None or value > extreme else extreme
        else:
            try:
                total += int(value)
            except ValueError:
                pass
    if current is not None:
        emit(current, total, extreme)


def emit(key, total, extreme):
    if key.startswith("min:") or key.startswith("max:"):
        sys.stdout.write("%s\t%s\n" % (key, extreme))
    else:
        sys.stdout.write("%s\t%d\n" % (key, total))


if __name__ == "__main__":
    main()
