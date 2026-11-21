#!/usr/bin/env bash
# Pre-flight checks. Run this first on the Dataproc master node and read the output.
cd "$(dirname "$0")/.." || exit 1
. ./env.sh

echo "== 1. Who and where"
echo "login: $(whoami)   HDFS home: $HDFS_USER_DIR"

echo
echo "== 2. Local disk in your home folder (zip files are about 10.3 GB in total)"
df -h "$HOME" | sed 's/^/   /'

echo
echo "== 3. HDFS capacity (raw CSV is several times larger than the zips, and HDFS keeps replicas)"
hdfs dfs -df -h / 2>&1 | sed 's/^/   /'

echo
echo "== 4. Python and Hadoop Streaming"
echo "   python3: $(python3 --version 2>&1)"
echo "   streaming jar: ${STREAMING_JAR:-NOT FOUND}"

echo
echo "== 5. Can this cluster download straight from Citi Bike's public bucket?"
echo "   (HTTP 200 or 206 means you can skip uploading the zips from your computer)"
curl -sS -m 20 -o /dev/null -w "   HTTP %{http_code}\n" \
  "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip" -r 0-0 2>&1 | sed 's/^/   /'

echo
echo "== 6. Zip files already in $ZIP_DIR"
if [ -d "$ZIP_DIR" ]; then ls -lh "$ZIP_DIR" | sed 's/^/   /'; else echo "   (folder does not exist yet)"; fi
