#!/usr/bin/env bash
# Clean the raw Citi Bike CSVs with two Hadoop Streaming jobs.
#   Stage 1: validate rows, derive borough, remove duplicate ride_ids
#            -> $CB_CLEAN_TRIPS  (one row per trip)
#   Stage 2: count trips per borough x local date x hour
#            -> $CB_CLEAN_HOURLY (small table used for the weather join)
# Produces clean_summary.txt, borough_hour.tsv and borough_hour_sample.tsv
# in the project folder.
set -o pipefail
cd "$(dirname "$0")/.." || exit 1
. ./env.sh
[ -z "$STREAMING_JAR" ] && { echo "Hadoop Streaming jar not found. Set STREAMING_JAR." >&2; exit 1; }
mkdir -p logs

echo "Removing any previous clean output under $CB_ROOT/clean"
hdfs dfs -rm -r -f -skipTrash "$CB_CLEAN_TRIPS" "$CB_CLEAN_HOURLY" >/dev/null 2>&1

echo "== Stage 1: validate, add borough, deduplicate"
"$HADOOP_BIN" jar "$STREAMING_JAR" \
  -D mapreduce.job.name="citibike-clean-stage1" \
  -D mapreduce.job.reduces="$NUM_REDUCERS" \
  -files clean/clean_mapper.py,clean/clean_reducer.py,common/cb.py,common/boroughs.py,common/borough_data.py \
  -cmdenv PYTHONPATH=. \
  -mapper "python3 clean_mapper.py" \
  -reducer "python3 clean_reducer.py" \
  -input "$CB_RAW/*/*.csv" \
  -output "$CB_CLEAN_TRIPS" 2>&1 | tee logs/clean_stage1.log || { echo "Stage 1 failed, see logs/clean_stage1.log" >&2; exit 1; }

echo "== Stage 2: aggregate to borough x hour"
"$HADOOP_BIN" jar "$STREAMING_JAR" \
  -D mapreduce.job.name="citibike-clean-stage2" \
  -D mapreduce.job.reduces="$NUM_REDUCERS_AGG" \
  -files clean/agg_mapper.py,clean/agg_reducer.py \
  -cmdenv PYTHONPATH=. \
  -mapper "python3 agg_mapper.py" \
  -reducer "python3 agg_reducer.py" \
  -input "$CB_CLEAN_TRIPS" \
  -output "$CB_CLEAN_HOURLY" 2>&1 | tee logs/clean_stage2.log || { echo "Stage 2 failed, see logs/clean_stage2.log" >&2; exit 1; }

hdfs dfs -cat "$CB_CLEAN_HOURLY/part-*" | LC_ALL=C sort -t$'\t' -k1,1 -k2,2 -k3,3n > borough_hour.tsv
head -n 200 borough_hour.tsv > borough_hour_sample.tsv

{
  echo "Citi Bike cleaning summary ($(date '+%Y-%m-%d %H:%M %Z'))"
  echo
  echo "Counters from stage 1 (reasons rows were rejected, and rows kept):"
  grep -hE '^[[:space:]]*(rows_read|skipped_header_line|rejected_[a-z0-9_]+|kept[a-zA-Z0-9_]*|duplicates_removed)=[0-9]+' \
      logs/clean_stage1.log | sed 's/^[[:space:]]*/  /' | sort -u
  echo
  echo "Borough x hour table (stage 2):"
  echo "  rows: $(wc -l < borough_hour.tsv)"
  awk -F'\t' '{t[$1]+=$8; y[$4]+=$8; all+=$8}
       END {printf "  total trips: %d\n", all;
            for (b in t) printf "  trips in %s: %d\n", b, t[b];
            for (k in y) printf "  trips in %s: %d\n", k, y[k]}' borough_hour.tsv | sort
} > clean_summary.txt

echo
cat clean_summary.txt
echo
echo "Trip-level clean data: $CB_CLEAN_TRIPS"
echo "Borough x hour table:  $CB_CLEAN_HOURLY  (also saved as borough_hour.tsv)"
