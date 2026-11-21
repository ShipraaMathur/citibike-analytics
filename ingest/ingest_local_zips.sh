#!/usr/bin/env bash
# Load every zip in $ZIP_DIR (default ~/citibike_zips) into HDFS under
# $CB_RAW/<year>/. Zips are processed one at a time. Set DELETE_ZIPS=1 to
# remove each local zip after it loads successfully, which keeps local disk
# use low.
#
# Usage:  bash ingest/ingest_local_zips.sh [--dry-run]
cd "$(dirname "$0")/.." || exit 1
. ./env.sh

extra=()
[ "${1:-}" = "--dry-run" ] && extra+=(--dry-run)
[ "${DELETE_ZIPS:-0}" = "1" ] && extra+=(--delete-zip-after)

shopt -s nullglob
zips=("$ZIP_DIR"/*.zip)
if [ ${#zips[@]} -eq 0 ]; then
  echo "No zip files found in $ZIP_DIR. Upload them there first (see README step 3)." >&2
  exit 1
fi

mkdir -p "$HOME/citibike_tmp"
fail=0
for z in "${zips[@]}"; do
  python3 ingest/zip_to_hdfs.py --raw-dir "$CB_RAW" --tmp-dir "$HOME/citibike_tmp" "${extra[@]}" "$z" || fail=1
done

echo
echo "== HDFS contents"
hdfs dfs -du -h "$CB_RAW/*/*" 2>/dev/null
[ $fail -eq 0 ] && echo "Ingest finished without errors." || echo "Some files failed. Re-run this script; finished files are skipped."
exit $fail
