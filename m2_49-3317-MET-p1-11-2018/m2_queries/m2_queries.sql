-- Query 1
-- All trip info(location,tip amount,etc) for the 20 highest trip distances
SELECT * FROM green_taxi_11_2018 ORDER BY trip_distance DESC LIMIT 20;

-- Query 2
-- What is the average fare amount per payment type?
SELECT AVG(fare_amount) as average_fare_amount, payment_type
FROM
	(SELECT fare_amount,
		CASE WHEN payment_type_cash = 1 THEN 'Cash'
			 WHEN payment_type_credit_card= 1 THEN 'Credit Card'
             WHEN payment_type_no_charge= 1 THEN 'No Charge'
             WHEN payment_type_dispute= 1 THEN 'Dispute'
             ELSE 'Unknown' END as payment_type 
	 FROM green_taxi_11_2018) dt 
GROUP BY payment_type;

-- Query 3
-- On average, which city tips the most.
SELECT AVG(tip_amount), city
FROM (
	SELECT split_part(pu_location, ',', 1) as city, tip_amount
	FROM green_taxi_11_2018) dt
GROUP BY city
ORDER BY AVG(tip_amount) DESC
LIMIT 1;

-- Query 4
-- On average, which city tips the least.
SELECT AVG(tip_amount), city
FROM (
	SELECT split_part(pu_location, ',', 1) as city, tip_amount
	FROM green_taxi_11_2018) dt
GROUP BY city
ORDER BY AVG(tip_amount) ASC
LIMIT 1;

-- Query 5
-- What is the most frequent destination on the weekend.
SELECT COUNT(*) tripcount, do_location 
FROM green_taxi_11_2018
WHERE EXTRACT(DOW FROM CAST(lpep_pickup_datetime as timestamp)) IN (0, 6) -- Weekend is Sunday(0) and Saturday(6) in USA
GROUP BY do_location
ORDER BY COUNT(*) DESC
LIMIT 1;

-- Query 6
-- On average, which trip type travels longer distances.
SELECT AVG(trip_distance) as average_trip_distance, trip_type
FROM
(SELECT trip_distance, CASE
	WHEN trip_type_dispatch = 1 THEN 'Dispatch'
	WHEN "trip_type_street-hail" = 1 THEN 'Street Hail'
	ELSE 'Unknown' 
	END as trip_type FROM green_taxi_11_2018
) dt
GROUP BY trip_type
ORDER BY AVG(trip_distance) DESC
LIMIT 1;


-- Query 7
-- between 4pm and 6pm what is the average fare amount.
SELECT AVG(fare_amount) as average_fare_amount
FROM green_taxi_11_2018
WHERE EXTRACT(HOUR FROM CAST(lpep_pickup_datetime as timestamp)) BETWEEN 16 AND 17
or (EXTRACT(HOUR FROM CAST(lpep_pickup_datetime as timestamp)) = 18 
and EXTRACT(MINUTE FROM CAST(lpep_pickup_datetime as timestamp)) = 0);
