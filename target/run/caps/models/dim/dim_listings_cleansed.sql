
  create or replace   view AIRBNB.DEV.dim_listings_cleansed
  
   as (
    

WITH  __dbt__cte__src_listings as (
WITH raw_listings AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_LISTINGS
)
SELECT

    id AS listing_id,
    name AS hotel_name,
    listing_url as hotel_url,
    room_type,
    minimum_nights,
    host_id,
    price ,
    cast(created_at as date) as created_at,
    cast(updated_at as date) as updated_at,

FROM
    raw_listings
), src_listings AS (
  SELECT
    *
  FROM
    __dbt__cte__src_listings
)
SELECT
  MD5(CONCAT(listing_id, created_at)) AS listing_sk,
  listing_id,
  hotel_name,
  room_type,
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  price,
  created_at,
  updated_at
FROM
  src_listings
  );

