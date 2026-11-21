#!/usr/bin/env bash
# OPTIONAL. Only use this if check_cluster.sh printed HTTP 200 or 206 for the
# Citi Bike bucket. It downloads the same files you already have on your
# computer, directly onto the cluster, so nothing has to be uploaded.
# Interrupted downloads resume when you run the script again.
cd "$(dirname "$0")/.." || exit 1
. ./env.sh
mkdir -p "$ZIP_DIR"
files="2023-citibike-tripdata.zip"
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do files="$files 2024$m-citibike-tripdata.zip"; done
for f in $files; do
  echo "== $f"
  curl -fL --retry 5 --retry-delay 5 -C - -o "$ZIP_DIR/$f" "https://s3.amazonaws.com/tripdata/$f" || {
    echo "download failed for $f" >&2; exit 1; }
done
ls -lh "$ZIP_DIR"
