-- Hive external tables over the cleaned Citi Bike data.
-- Run with your HDFS home folder passed in, for example:
--   hive -hivevar base=/user/sam9907_nyu_edu/citibike -f hive/create_tables.sql
-- (Beeline: beeline -u <jdbc url> --hivevar base=/user/sam9907_nyu_edu/citibike -f hive/create_tables.sql)

CREATE DATABASE IF NOT EXISTS citibike;
USE citibike;

-- One row per cleaned trip (output of clean stage 1)
DROP TABLE IF EXISTS trips_clean;
CREATE EXTERNAL TABLE trips_clean (
  ride_id        STRING,
  rideable_type  STRING,
  started_at     STRING,   -- New York local time, yyyy-MM-dd HH:mm:ss
  ended_at       STRING,
  duration_sec   INT,
  start_lat      DOUBLE,
  start_lng      DOUBLE,
  borough        STRING,
  member_casual  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:base}/clean/trips';

-- One row per borough x local date x hour (output of clean stage 2)
-- This is the table to join with weather, taxi, subway and 911 data.
DROP TABLE IF EXISTS borough_hour;
CREATE EXTERNAL TABLE borough_hour (
  borough            STRING,
  trip_date          STRING,   -- New York local date, yyyy-MM-dd
  trip_hour          INT,      -- New York local hour 0-23
  trip_year          INT,
  trip_month         INT,
  day_of_week        STRING,   -- Mon..Sun
  is_weekend         INT,
  trips              BIGINT,
  member_trips       BIGINT,
  casual_trips       BIGINT,
  electric_trips     BIGINT,
  classic_trips      BIGINT,
  other_type_trips   BIGINT,
  total_duration_sec BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:base}/clean/borough_hour';

-- Quick checks
SELECT borough, SUM(trips) AS trips FROM borough_hour GROUP BY borough ORDER BY trips DESC;
SELECT trip_year, SUM(trips) AS trips FROM borough_hour GROUP BY trip_year;
