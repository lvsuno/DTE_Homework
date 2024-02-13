-- Prepare the data

-- Creating external table referring to gcs path

CREATE OR REPLACE EXTERNAL TABLE `dte-course.ny_taxi.external_green_tripdata_2022`(
   VendorID Int64,
   lpep_pickup_datetime TIMESTAMP, 
   lpep_dropoff_datetime TIMESTAMP,
   passenger_count Int64,
   trip_distance FLOAT64,
   RatecodeID  Int64,
   store_and_fwd_flag STRING,
   PULocationID  Int64,
   DOLocationID Int64,
   payment_type Int64,
   fare_amount FLOAT64,
   extra float64,
   mta_tax float64,
   tip_amount float64,
   tolls_amount float64,
   improvement_surcharge float64,
   total_amount float64,
   trip_type Int64,
   congestion_surcharge float64
)
OPTIONS (
  format = 'parquet',
  uris = ['gs://data_warehouse_homework/green_taxi_2022.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dte-course.ny_taxi.green_tripdata_2022 AS
SELECT * FROM dte-course.ny_taxi.external_green_tripdata_2022;

-- Question 1
-- Number of record 
SELECT COUNT(*) FROM dte-course.ny_taxi.green_tripdata_2022;

-- Question 2
-- Count distinct number of PULocationID from the external table
SELECT COUNT(DISTINCT PULocationID) FROM dte-course.ny_taxi.external_green_tripdata_2022;

-- Count distinct number of PULocationID from the Materialized Table
SELECT COUNT(DISTINCT PULocationID) FROM dte-course.ny_taxi.green_tripdata_2022;

-- Question 3
-- How many records have a fare_amount of 0?
SELECT COUNT(*) FROM dte-course.ny_taxi.external_green_tripdata_2022 where fare_amount=0;

-- Question 4
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE dte-course.ny_taxi.green_tripdata_partitoned_clustered_2022
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM dte-course.ny_taxi.external_green_tripdata_2022;

-- Question 5
-- Count distinct number of PULocationID from the Materialized Table
SELECT COUNT(DISTINCT PULocationID) FROM dte-course.ny_taxi.green_tripdata_2022 
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Count distinct number of PULocationID from the partitioned Table
SELECT COUNT(DISTINCT PULocationID) FROM dte-course.ny_taxi.green_tripdata_partitoned_clustered_2022 
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


