#!/usr/bin/env bash
# Shared settings. Every script in this project sources this file.
# Override any value by exporting it before running a script.

# HDFS locations (your home directory is /user/<login>, for example /user/sam9907_nyu_edu)
export HDFS_USER_DIR="${HDFS_USER_DIR:-/user/$(whoami)}"
export CB_ROOT="${CB_ROOT:-$HDFS_USER_DIR/citibike}"
export CB_RAW="$CB_ROOT/raw"
export CB_PROFILE_OUT="$CB_ROOT/profile_out"
export CB_CLEAN_TRIPS="$CB_ROOT/clean/trips"
export CB_CLEAN_HOURLY="$CB_ROOT/clean/borough_hour"

# Local folder on the cluster where the zip files are placed
export ZIP_DIR="${ZIP_DIR:-$HOME/citibike_zips}"

# Reducer counts: stage 1 shuffles every trip, stage 2 is tiny
export NUM_REDUCERS="${NUM_REDUCERS:-16}"
export NUM_REDUCERS_AGG="${NUM_REDUCERS_AGG:-4}"

HADOOP_BIN="${HADOOP_BIN:-hadoop}"
export HADOOP_BIN

# Locate the Hadoop Streaming jar if it was not given
if [ -z "${STREAMING_JAR:-}" ]; then
  for candidate in \
      /usr/lib/hadoop/hadoop-streaming.jar \
      /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
      /usr/lib/hadoop/tools/lib/hadoop-streaming*.jar \
      /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar; do
    if [ -f "$candidate" ]; then STREAMING_JAR="$candidate"; break; fi
  done
fi
if [ -z "${STREAMING_JAR:-}" ]; then
  STREAMING_JAR="$(find /usr/lib /opt -name 'hadoop-streaming*.jar' 2>/dev/null | head -n 1)"
fi
export STREAMING_JAR
