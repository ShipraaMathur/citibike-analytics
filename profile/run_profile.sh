#!/usr/bin/env bash
# Profile the raw Citi Bike CSVs with one Hadoop Streaming job.
# Produces profile_results.tsv in the project folder.
set -o pipefail
cd "$(dirname "$0")/.." || exit 1
. ./env.sh
[ -z "$STREAMING_JAR" ] && { echo "Hadoop Streaming jar not found. Set STREAMING_JAR." >&2; exit 1; }
mkdir -p logs

echo "Removing any previous profile output at $CB_PROFILE_OUT"
hdfs dfs -rm -r -f -skipTrash "$CB_PROFILE_OUT" >/dev/null 2>&1

"$HADOOP_BIN" jar "$STREAMING_JAR" \
  -D mapreduce.job.name="citibike-profile" \
  -D mapreduce.job.reduces=1 \
  -files profile/profile_mapper.py,profile/profile_reducer.py,common/cb.py \
  -cmdenv PYTHONPATH=. \
  -mapper "python3 profile_mapper.py" \
  -combiner "python3 profile_reducer.py" \
  -reducer "python3 profile_reducer.py" \
  -input "$CB_RAW/*/*.csv" \
  -output "$CB_PROFILE_OUT" 2>&1 | tee logs/profile.log || { echo "Profile job failed, see logs/profile.log" >&2; exit 1; }

hdfs dfs -cat "$CB_PROFILE_OUT/part-*" | LC_ALL=C sort > profile_results.tsv
echo
echo "Wrote profile_results.tsv ($(wc -l < profile_results.tsv) metrics). Headline numbers:"
grep -E '^(rows_total|rows_header|rows_bad_column_count|invalid_started_at|invalid_ended_at|min:started_at|max:started_at)' profile_results.tsv
