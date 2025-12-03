-- CREATE DATABASE HOTEL_INFO_DB

CREATE OR REPLACE FILE FORMAT FF_CSV
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')



CREATE OR REPLACE STAGE STG_HOTEL_BOOKINGS
    FILE_FORMAT = FF_CSV;



CREATE OR REPLACE TABLE BRONZE_HOTEL(
    booking_id         STRING,
    hotel_id	       STRING,
    hotel_city	       STRING,
    customer_id	       STRING,
    customer_name	   STRING,
    customer_email	   STRING,
    check_in_date	   STRING,
    check_out_date	   STRING,
    room_type	       STRING,
    num_guests	       STRING,
    total_amount	   STRING,
    currency	       STRING,
    booking_status     STRING
)

COPY INTO BRONZE_HOTEL
FROM @STG_HOTEL_BOOKINGS
FILE_FORMAT = (FORMAT_NAME = FF_CSV)
ON_ERROR = 'CONTINUE';

SELECT * FROM BRONZE_HOTEL

CREATE TABLE SILVER_HOTEL_BOOKINGS(
    booking_id         VARCHAR,
    hotel_id	       VARCHAR,
    hotel_city	       VARCHAR,
    customer_id	       VARCHAR,
    customer_name	   VARCHAR,
    customer_email	   VARCHAR,
    check_in_date	   DATE,
    check_out_date	   DATE,
    room_type	       VARCHAR,
    num_guests	       INTEGER,
    total_amount	   FLOAT,
    currency	       VARCHAR,
    booking_status     VARCHAR
)



SELECT customer_email FROM BRONZE_HOTEL
WHERE NOT (customer_email like '%@_%.%') OR (customer_email IS NULL);

SELECT total_amount FROM BRONZE_HOTEL
WHERE TRY_TO_NUMBER(total_amount) < 0;


SELECT check_in_date, check_out_date FROM BRONZE_HOTEL
WHERE TRY_TO_DATE(check_in_date) > TRY_TO_DATE(check_out_date)


-- check fpor the booking status
SELECT DISTINCT booking_status FROM BRONZE_HOTEL


INSERT INTO SILVER_HOTEL_BOOKINGS
SELECT 
    booking_id,
    hotel_id,
    INITCAP(TRIM(hotel_city)) AS hotel_city,
    customer_id,
    INITCAP(TRIM(customer_name)) AS customer_name,
    CASE
        WHEN customer_email LIKE  '%@_%.%' THEN LOWER(TRIM(customer_email))
        ELSE NULL
    END AS customer_email,
    TRY_TO_DATE(NULLIF(check_in_date, '')) AS check_in_date,
    TRY_TO_DATE(NULLIF(check_out_date, '')) AS check_out_date,
    room_type,
    num_guests,
    ABS(TRIM(total_amount)) AS total_amount,
    currency,
    CASE
        WHEN LOWER(booking_status) IN ('confirmed', 'confirmeeed') THEN 'Confirmed'
        ELSE booking_status
    END AS booking_status
    FROM BRONZE_HOTEL
    WHERE TRY_TO_DATE(check_in_date) IS NOT NULL
            AND TRY_TO_DATE(check_out_date) IS NOT NULL
            AND TRY_TO_DATE(check_in_date) <= TRY_TO_DATE(check_out_date) 



SELECT * FROM SILVER_HOTEL_BOOKINGS



CREATE OR REPLACE TABLE GOLD_AGG_DAILY_BOOKINGS AS 
SELECT check_in_date AS date, COUNT(*) AS total_booking, SUM(total_amount) AS total_revenue
FROM SILVER_HOTEL_BOOKINGS
GROUP BY check_in_date
ORDER BY date DESC



CREATE TABLE GOLD_AGG_HOSTEL_CITY_SALES AS 
SELECT hostel_city, SUM(total_amount) AS total_revenue 
FROM SILVER_HOTEL_BOOKINGS
GROUP BY hostel_city
ORDER BY total_revenue DESC


CREATE TABLE GOLD_BOOKING_CLEAN AS
SELECT 
    booking_id         VARCHAR,
    hotel_id	       VARCHAR,
    hotel_city	       VARCHAR,
    customer_id	       VARCHAR,
    customer_name	   VARCHAR,
    customer_email	   VARCHAR,
    check_in_date	   DATE,
    check_out_date	   DATE,
    room_type	       VARCHAR,
    num_guests	       INTEGER,
    total_amount	   FLOAT,
    currency	       VARCHAR,
    booking_status     VARCHAR
FROM SILVER_HOTEL_BOOKINGS