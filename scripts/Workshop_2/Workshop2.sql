-- Question 1 

-- Average time view
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





WITH t AS (
        SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time
        FROM trip_data
    )
    select pick_taxi.Zone as pickup_zone, 
    count(*) as num_trips
    from t, trip_data
    Join taxi_zone  as pick_taxi
        ON trip_data.pulocationid = pick_taxi.location_id
    Where trip_data.tpep_pickup_datetime > t.latest_pickup_time - interval '17 hour'
    group by  pick_taxi.Zone
    order by num_trips DESC
    LIMIT 3;
