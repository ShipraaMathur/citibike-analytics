#!/usr/bin/env bash
# End-to-end test on this computer with synthetic data and a stand-in Hadoop.
# It runs the real shell scripts and Python jobs; only `hdfs` and `hadoop` are
# replaced by test/fake_hadoop_tools.py. Run from anywhere:
#   bash test/run_local_test.sh
set -e
SRC="$(cd "$(dirname "$0")/.." && pwd)"
T="$(mktemp -d)"
echo "Working in $T"

# Work on a copy so test output never lands in the real project folder
cp -r "$SRC" "$T/proj"
rm -rf "$T/proj/logs" "$T"/proj/*.tsv "$T/proj/clean_summary.txt"

mkdir -p "$T/zips" "$T/home" "$T/hdfs"
python3 "$T/proj/test/make_synthetic.py" "$T/syn" >/dev/null
cp "$T"/syn/*.zip "$T/zips/"

export PATH="$T/proj/test/bin:$PATH"
export FAKE_HDFS_ROOT="$T/hdfs"
export HOME="$T/home"
export ZIP_DIR="$T/zips"
export HDFS_USER_DIR="/user/tester"
export STREAMING_JAR="/fake/hadoop-streaming.jar"
export NUM_REDUCERS=3
export NUM_REDUCERS_AGG=2
cd "$T/proj"

echo; echo "### dry run"
bash ingest/ingest_local_zips.sh --dry-run

echo; echo "### ingest"
bash ingest/ingest_local_zips.sh

echo; echo "### ingest again (should skip everything)"
bash ingest/ingest_local_zips.sh | grep -E "SKIP|UPLOADED" | sort | uniq -c | awk '{print "  " $0}'

echo; echo "### profile"
bash profile/run_profile.sh > "$T/profile.out" 2>&1 || { tail -n 30 "$T/profile.out"; exit 1; }
head -5 profile_results.tsv

echo; echo "### clean"
bash clean/run_clean.sh > "$T/clean.out" 2>&1 || { tail -n 30 "$T/clean.out"; exit 1; }
cat clean_summary.txt

echo; echo "### verify"
python3 test/verify.py "$T/syn/expected.json" "$T/proj" "$T/hdfs" "/user/tester/citibike"
echo "Test directory kept at $T"
