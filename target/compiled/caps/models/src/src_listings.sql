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