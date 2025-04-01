
  create or replace   view AIRBNB.DEV.src_host
  
   as (
    SELECT 
    fact_id,
    listing_id,
    listing_url,
    name,
    room_type,
    minimum_nights,
    host_id,
    host_name,
    is_superhost,
    price,
    cast(review_date as date) as review_date,
    reviewer_name,
    review_comments,
    sentiment,
    created_at,
    updated_at

FROM
        AIRBNB.RAW.AIRBNB_TX_TABLE1
  );

