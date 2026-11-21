# Citi Bike Trip Data Pipeline

This repository contains my Citi Bike contribution to Group 3's CSCI-GA.2436 project, *Weather, 911 Calls and Transportation in NYC*. I am Shipra Anil Mathur (sam9907).

For this part of the project, I loaded Citi Bike trip data from 2023 and 2024 into HDFS on NYU Dataproc, profiled the raw data with Hadoop Streaming, and cleaned it for the team's borough-by-hour analysis. I used Python 3 MapReduce jobs throughout, as required by the project proposal.

## Results from my cluster run

| Stage | Result |
| --- | --- |
| Raw data | 79,410,195 trips in 90 CSV files, totaling 15.53 GB |
| Profiling | No malformed rows or bad timestamps; 48,033 trips had no start location and 37,576 lasted more than 24 hours |
| Cleaning | 86,425 rows removed (0.11%); no duplicate ride IDs found; borough derived from the start coordinates |
| Final output | 79,323,770 cleaned trips and 71,093 borough-by-hour rows |

## Dataset

I used 13 Citi Bike trip-data archives:

- `2023-citibike-tripdata.zip`, a yearly archive with monthly folders
- `202401-citibike-tripdata.zip` through `202412-citibike-tripdata.zip`

Together, the ZIP files are about 10.3 GB. Inside them are 90 CSV files with 13 columns per trip. The ingestion script reads the CSV files directly from the ZIP archives and streams them into HDFS, so the cluster does not need to unpack the archives on its local disk. It also handles nested ZIP files, split monthly files, and macOS `__MACOSX` entries.

The source data does not include a borough column. I derive the borough from the start latitude and longitude during cleaning. Citi Bike timestamps are published in New York local time with milliseconds and no time-zone marker, so I keep them in local time for the team's borough-by-hour join.

## Reproducing the pipeline

### 1. Put the project on the Dataproc cluster

I uploaded `citibike-analytics.tar.gz` to the Dataproc master node and unpacked it there:

```bash
tar xzf citibike-analytics.tar.gz
cd citibike-analytics
```

All remaining commands are run from this folder.

### 2. Check the cluster

```bash
bash ingest/check_cluster.sh
```

This script checks local disk space, HDFS capacity, Python 3, the Hadoop Streaming JAR, and whether the cluster can reach the Citi Bike download bucket. The raw CSV files are much larger than the ZIP archives, and HDFS keeps three replicas by default, so I check for at least 100 GB of free HDFS space before loading the data.

If the download check returns `HTTP 200` or `HTTP 206`, the cluster can fetch the data directly. Any other result means the ZIP files need to be uploaded first.

### 3. Get the source archives

When the cluster has bucket access, I download the archives directly on the master node:

```bash
bash ingest/fetch_zips.sh
```

The files are saved in `~/citibike_zips`. The script supports resume, so rerunning it continues an interrupted download.

If direct download is unavailable, I create the local ZIP folder first:

```bash
mkdir -p ~/citibike_zips
```

I can then upload the archives through the Dataproc SSH page, with `gcloud compute scp`, or through Cloud Storage. For example, from my computer I use the following pattern, replacing the capitalized values with the cluster details:

```bash
cd "/Users/shipramathur/Downloads/RBDA Project"
gcloud compute scp --project PROJECT_ID --zone ZONE *.zip CLUSTER_NAME-m:~/citibike_zips/
```

After uploading, I check that the archives arrived with the expected sizes:

```bash
ls -l ~/citibike_zips
```

### 4. Load the archives into HDFS

```bash
DELETE_ZIPS=1 bash ingest/ingest_local_zips.sh
```

`DELETE_ZIPS=1` removes each local archive only after its CSV files have been loaded successfully. I use it to keep local disk use low. Leaving out that variable keeps the ZIP files.

For a preview of the load, I can run:

```bash
bash ingest/ingest_local_zips.sh --dry-run
```

The ingestion script is safe to rerun: HDFS files with the same path and size are skipped. My completed run stored 90 raw files under `/user/<login>/citibike/raw/2023/` and `/user/<login>/citibike/raw/2024/`.

### 5. Profile the raw data

```bash
bash profile/run_profile.sh
```

This runs one Hadoop Streaming job and writes `profile_results.tsv`. The mapper checks CSV structure, required values, timestamps, trip duration, start coordinates, bike type, rider type, start month, and start hour. The reducer aggregates the results into 73 metrics.

For my run, the profiling job processed 79,410,195 trips in 1 minute and 41 seconds. The output confirmed that the file structure and timestamps were clean, while identifying the value-level issues used in the cleaning rules.

### 6. Clean and aggregate the data

```bash
bash clean/run_clean.sh
```

This script runs two MapReduce jobs.

1. Stage 1 validates every trip, derives the borough from the start coordinates, removes rows that fail the cleaning rules, and keeps one deterministic record per `ride_id`. It writes one cleaned record per trip to `/user/<login>/citibike/clean/trips`.
2. Stage 2 aggregates the cleaned trips by borough, local date, and hour. It writes the final table to `/user/<login>/citibike/clean/borough_hour`.

The script also creates these files in the project folder:

- `clean_summary.txt`, which lists every rejection reason and final total
- `borough_hour.tsv`, the full borough-by-hour output
- `borough_hour_sample.tsv`, the first 200 output rows

#### Cleaning rules

| Rule | Purpose |
| --- | --- |
| Line does not have 13 columns | Reject unusable input rows |
| Empty `ride_id` | Keep every trip identifiable for deduplication |
| Invalid `started_at` or `ended_at` | Protect the hour key and duration calculation |
| Start year is not 2023 or 2024 | Keep the project period consistent |
| End time is before start time | Remove impossible durations |
| Duration is under 60 seconds | Apply Citi Bike's false-start threshold |
| Duration is over 24 hours | Remove likely undocked bikes rather than real rides |
| Start latitude or longitude is empty | Remove trips that cannot be placed in a borough |
| Start point is outside the New York area box | Remove invalid coordinates |
| `member_casual` is not `member` or `casual` | Protect rider-type counts |
| Repeated `ride_id` | Keep one copy of a duplicated trip |

The duration thresholds are constants in `common/cb.py` and can be changed if the group agrees on different limits.

#### Borough assignment

I match each start location to simplified NYC borough boundaries in `common/borough_data.py`. Points within 150 m of a boundary are assigned to the nearest borough to account for GPS noise near piers. Jersey City and Hoboken trips are labeled `Non-NYC`. I keep them in the output so total counts remain complete, but they should be filtered out before joining to NYC borough weather data.

#### Time zone and daylight saving time

I keep Citi Bike timestamps in their published New York local time. The weather data owner needs to convert NOAA ISD timestamps from UTC before the group joins the datasets.

- On November 5, 2023 and November 3, 2024, the 1:00 a.m. hour happened twice. Citi Bike data does not distinguish the two occurrences, so both are counted in the same local hour.
- A trip that crosses a daylight-saving time change can have a duration off by one hour. This affects very few trips and is not corrected.

### 7. Create the Hive tables

```bash
hive -hivevar base=/user/$(whoami)/citibike -f hive/create_tables.sql
```

This creates the `citibike` database with external tables named `trips_clean` and `borough_hour`. I use `borough_hour` for the team’s borough-by-hour join.

## Troubleshooting

| Symptom | Response |
| --- | --- |
| `Hadoop Streaming jar not found` | Run `find /usr/lib -name 'hadoop-streaming*.jar'`, then set `STREAMING_JAR` to the returned path and rerun. |
| `python3: command not found` inside a task | Configure the jobs to use an interpreter available on the worker nodes. |
| Ingestion stops partway | Rerun the same command; completed HDFS files are skipped. |
| `No space left on device` | Use `DELETE_ZIPS=1`, or remove uploaded ZIP files after they are loaded. |
| HDFS quota exceeded | Check usage with `hdfs dfs -du -h /user/$(whoami)` and remove old outputs with `hdfs dfs -rm -r -skipTrash <path>`. |
| A job fails | Check the final 40 lines of `logs/profile.log`, `logs/clean_stage1.log`, or `logs/clean_stage2.log`. |
| A job is slow | Increase `NUM_REDUCERS`, for example: `NUM_REDUCERS=32 bash clean/run_clean.sh`. |

Rerunning `bash clean/run_clean.sh` starts the cleaning stage over. It removes only its previous output and never changes the raw HDFS folder.

## Testing

Before the Dataproc run, I tested the full pipeline end to end with synthetic Citi Bike archives. The test data includes a yearly ZIP with month folders and split parts, monthly ZIP files, a nested ZIP, macOS junk entries, quoted station names, and about 35 injected defects. All 30 checks in `test/verify.py` passed, including exact borough-by-hour totals.

To run the local test again:

```bash
bash test/run_local_test.sh
```

I then ran the pipeline on the NYU Dataproc cluster with the full 2023-2024 Citi Bike dataset. The profiling and two cleaning jobs completed successfully, and the output totals in this README come from that run.

## Project layout

```text
env.sh                    shared settings such as HDFS paths and the Streaming JAR
ingest/                   cluster check, ZIP download, HDFS ingestion, and ZIP reader
profile/                  profiling launcher, mapper, and reducer
clean/                    cleaning launcher, stage 1 mapper and reducer, aggregation mapper and reducer
common/                   parsing rules and borough lookup data
hive/create_tables.sql    external tables for the cleaned data
tools/build_boroughs.py   builds borough_data.py from a boundary GeoJSON file
test/                     synthetic-data generator, stand-in Hadoop tools, and end-to-end test
```
