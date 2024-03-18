# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Recreate taxi zone table
psql -f risingwave-sql/table/taxi_zone.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

### Response

```sql
CREATE MATERIALIZED VIEW average_time AS
    WITH t AS (
        SELECT (tpep_dropoff_datetime - tpep_pickup_datetime) AS trip_duration,
        dolocationid, pulocationid
        FROM trip_data
    )
    select pick_taxi.Zone as pickup_zone, drop_taxi.Zone as drop_zone,
    AVG(trip_duration) as average_duration
    from t
    Join taxi_zone  as drop_taxi
        ON t.dolocationid = drop_taxi.location_id
    Join taxi_zone  as pick_taxi
        ON t.pulocationid = pick_taxi.location_id
    group by  pick_taxi.Zone, drop_taxi.Zone;

-- Min time view
CREATE MATERIALIZED VIEW min_time AS
    WITH t AS (
        SELECT (tpep_dropoff_datetime - tpep_pickup_datetime) AS trip_duration,
        dolocationid, pulocationid
        FROM trip_data
    )
    select pick_taxi.Zone as pickup_zone, drop_taxi.Zone as drop_zone,
    MIN(trip_duration) as average_duration
    from t
    Join taxi_zone  as drop_taxi
        ON t.dolocationid = drop_taxi.location_id
    Join taxi_zone  as pick_taxi
        ON t.pulocationid = pick_taxi.location_id
    group by  pick_taxi.Zone, drop_taxi.Zone;


-- Max time view
CREATE MATERIALIZED VIEW max_time AS
    WITH t AS (
        SELECT (tpep_dropoff_datetime - tpep_pickup_datetime) AS trip_duration,
        dolocationid, pulocationid
        FROM trip_data
    )
    select pick_taxi.Zone as pickup_zone, drop_taxi.Zone as drop_zone,
    MAX(trip_duration) as average_duration
    from t
    Join taxi_zone  as drop_taxi
        ON t.dolocationid = drop_taxi.location_id
    Join taxi_zone  as pick_taxi
        ON t.pulocationid = pick_taxi.location_id
    group by  pick_taxi.Zone, drop_taxi.Zone;

-- This Query answer the question
with t as (
    SELECT MAX(average_duration) as max_du FROM average_time
)

SELECT pickup_zone, drop_zone FROM average_time, t
WHERE average_duration=t.max_du;

--  pickup_zone   | drop_zone 
--  --------------+-----------
-- Yorkville East | Steinway
-- (1 row)

```


## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

### Response

``` sql
CREATE MATERIALIZED VIEW average_time_trips AS
    WITH t AS (
        SELECT (tpep_dropoff_datetime - tpep_pickup_datetime) AS trip_duration,
        dolocationid, pulocationid
        FROM trip_data
    )
    select pick_taxi.Zone as pickup_zone, drop_taxi.Zone as drop_zone,
    AVG(trip_duration) as average_duration, count(*) as num_trips
    from t
    Join taxi_zone  as drop_taxi
        ON t.dolocationid = drop_taxi.location_id
    Join taxi_zone  as pick_taxi
        ON t.pulocationid = pick_taxi.location_id
    group by  pick_taxi.Zone, drop_taxi.Zone;



with t as (
    SELECT MAX(average_duration) as max_du FROM average_time_trips
)

SELECT num_trips FROM average_time_trips, t
WHERE average_duration=t.max_du;


-- num_trips 
-- ---------
--        1

```

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

### Response 

```sql
WITH t AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
        FROM trip_data
    )
    select pick_taxi.Zone as pickup_zone, 
    count(*) as num_trips
    from t, trip_data
    Join taxi_zone  as pick_taxi
        ON trip_data.pulocationid = pick_taxi.location_id
    Where trip_data.tpep_pickup_datetime >= t.latest_pickup_time - interval '17 hour'
    group by  pick_taxi.Zone
    order by num_trips DESC
    LIMIT 3;

--        pickup_zone   | num_trips 
--  --------------------+-----------
--  LaGuardia Airport   |        19
--  Lincoln Square East |        17
--  JFK Airport         |        17
-- (3 rows)
```