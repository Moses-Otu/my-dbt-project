{{
  config(
    materialized = 'view'
    )
}}

WITH src_listings AS (
  SELECT
    *
  FROM
    {{ ref('src_listings') }}
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