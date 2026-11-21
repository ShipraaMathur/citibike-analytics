#!/usr/bin/env python3
"""Stream the CSV files inside Citi Bike zip archives straight into HDFS.

Nothing is unpacked onto the cluster's local disk. Each CSV member is read
from the zip and piped into `hdfs dfs -put -`. Nested zip files (a zip inside
a zip) are handled by copying the inner zip to a temporary file first.

Layout written: <raw-dir>/<year>/<csv file name>
The year comes from the yyyymm prefix of the CSV name, for example
202305-citibike-tripdata_2.csv goes to <raw-dir>/2023/.

Safe to re-run: a file already in HDFS with the same size is skipped.

Environment: HDFS_CMD overrides the `hdfs` executable (used by the tests).
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile

HDFS = os.environ.get("HDFS_CMD", "hdfs")
CHUNK = 4 * 1024 * 1024
YM = re.compile(r"(20\d\d)(0[1-9]|1[0-2])")


def hdfs(*args, **kw):
    return subprocess.run([HDFS, "dfs"] + list(args), **kw)


def hdfs_size(path):
    """Size in bytes of a file in HDFS, or None if it does not exist."""
    res = hdfs("-du", path, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
               universal_newlines=True)
    if res.returncode != 0 or not res.stdout.strip():
        return None
    try:
        return int(res.stdout.split()[0])
    except ValueError:
        return None


def year_for(csv_name, zip_label):
    for text in (csv_name, zip_label):
        m = YM.search(text)
        if m:
            return m.group(1)
        m = re.search(r"(20\d\d)", text)
        if m:
            return m.group(1)
    return "unknown"


class Stats(object):
    def __init__(self):
        self.uploaded = 0
        self.skipped = 0
        self.failed = 0
        self.bytes = 0
        self.seen = set()


def upload_member(zf, info, dest, args, stats):
    size = info.file_size
    if args.dry_run:
        print("DRY RUN  %s -> %s (%.1f MB)" % (info.filename, dest, size / 1e6))
        return
    if not args.force and hdfs_size(dest) == size:
        print("SKIP     %s (already in HDFS, %d bytes)" % (dest, size))
        stats.skipped += 1
        return
    proc = subprocess.Popen([HDFS, "dfs", "-put", "-f", "-", dest],
                            stdin=subprocess.PIPE)
    written = 0
    try:
        with zf.open(info) as src:
            while True:
                block = src.read(CHUNK)
                if not block:
                    break
                proc.stdin.write(block)
                written += len(block)
        proc.stdin.close()
    except (BrokenPipeError, OSError) as exc:
        print("FAILED   %s: %s" % (dest, exc), file=sys.stderr)
        proc.kill()
        proc.wait()
        stats.failed += 1
        return
    rc = proc.wait()
    if rc != 0 or written != size:
        print("FAILED   %s (exit %s, wrote %d of %d bytes)" % (dest, rc, written, size),
              file=sys.stderr)
        stats.failed += 1
        return
    print("UPLOADED %s (%.1f MB)" % (dest, size / 1e6))
    stats.uploaded += 1
    stats.bytes += size


def process_zip(zf, label, args, stats, depth=0):
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = info.filename
        base = os.path.basename(name)
        if not base or name.startswith("__MACOSX/") or base.startswith("._"):
            continue
        low = base.lower()
        if low.endswith(".csv"):
            year = year_for(base, label)
            if base in stats.seen:
                parent = re.sub(r"[^A-Za-z0-9]+", "_", os.path.dirname(name)).strip("_")
                base = "%s__%s" % (parent or "dup", base)
            stats.seen.add(base)
            process_dest = "%s/%s/%s" % (args.raw_dir.rstrip("/"), year, base)
            upload_member(zf, info, process_dest, args, stats)
        elif low.endswith(".zip") and depth < 3:
            fd, tmp = tempfile.mkstemp(suffix=".zip", dir=args.tmp_dir)
            os.close(fd)
            try:
                with zf.open(info) as src, open(tmp, "wb") as out:
                    shutil.copyfileobj(src, out, CHUNK)
                with zipfile.ZipFile(tmp) as inner:
                    process_zip(inner, base, args, stats, depth + 1)
            finally:
                os.remove(tmp)
        else:
            print("IGNORED  %s" % name)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("zips", nargs="+", help="zip files to load")
    ap.add_argument("--raw-dir", required=True,
                    help="HDFS directory for raw CSVs, e.g. /user/<id>/citibike/raw")
    ap.add_argument("--tmp-dir", default=tempfile.gettempdir(),
                    help="local directory for nested zips (default: system temp)")
    ap.add_argument("--force", action="store_true",
                    help="re-upload even when HDFS already has a same-size file")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would be uploaded without touching HDFS")
    ap.add_argument("--delete-zip-after", action="store_true",
                    help="remove each local zip once all of its files uploaded")
    args = ap.parse_args()

    if not args.dry_run:
        hdfs("-mkdir", "-p", args.raw_dir.rstrip("/"), check=True)

    exit_code = 0
    for path in args.zips:
        if not zipfile.is_zipfile(path):
            print("NOT A ZIP: %s" % path, file=sys.stderr)
            exit_code = 1
            continue
        stats = Stats()
        print("== %s" % path)
        with zipfile.ZipFile(path) as zf:
            process_zip(zf, os.path.basename(path), args, stats)
        print("   %d uploaded, %d skipped, %d failed, %.2f GB written"
              % (stats.uploaded, stats.skipped, stats.failed, stats.bytes / 1e9))
        if stats.failed:
            exit_code = 1
        elif args.delete_zip_after and not args.dry_run:
            os.remove(path)
            print("   removed local copy %s" % path)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
