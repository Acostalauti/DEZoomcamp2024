--SETUP:
-- Create an external table using the Green Taxi Trip Records Data for 2022.
-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412302.ny_taxi.external_green_tripdata_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-acostalauti/2022/green_tripdata_2022-*.parquet']
);

-- Check data

SELECT * FROM `ny_taxi.external_green_tripdata_2022` limit 10;

-- Create a table
CREATE OR REPLACE TABLE`terraform-demo-412302.ny_taxi.green_tripdata_2022` AS
SELECT * FROM `terraform-demo-412302.ny_taxi.external_green_tripdata_2022`;


--Question 1: What is count of records for the 2022 Green Taxi Data??

SELECT COUNT(*) AS total_rows
FROM `ny_taxi.external_green_tripdata_2022`;

-- RESPUESTA 1 840402

-- Question 2:
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny_taxi.external_green_tripdata_2022`;
-- 258
-- CONSUME 0 B

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny_taxi.green_tripdata_2022`;
-- 258
-- CONSUME 6,41MB

-- RESPUESTA 2 0 MB for the External Table and 6.41MB for the Materialized Table

-- Question 3:
-- How many records have a fare_amount of 0?

SELECT COUNT(*) AS zero_fare_count
FROM `ny_taxi.external_green_tripdata_2022`
WHERE fare_amount = 0;

-- RESPUESTA 1622

-- Question 4:
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID -- and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

CREATE TABLE `ny_taxi.optimized_green_tripdata_2022`
PARTITION BY date(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS SELECT * FROM `ny_taxi.green_tripdata_2022`;

--RESPUESTA Partition by lpep_pickup_datetime Cluster on PUlocationID

-- Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the -- from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

SELECT DISTINCT PULocationID
FROM `ny_taxi.green_tripdata_2022`
WHERE DATE(lpep_pickup_datetime) >= '2022-06-01' AND DATE(lpep_pickup_datetime) < '2022-06-30';

--This query will process 12.82 MB wh

SELECT DISTINCT PULocationID
FROM `ny_taxi.optimized_green_tripdata_2022`
WHERE DATE(lpep_pickup_datetime) >= '2022-06-01' AND DATE(lpep_pickup_datetime) < '2022-06-30';

-- This query will process 1.09 MB whe

-- RESULTADO Choose the answer which most closely matches.
-- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table 

