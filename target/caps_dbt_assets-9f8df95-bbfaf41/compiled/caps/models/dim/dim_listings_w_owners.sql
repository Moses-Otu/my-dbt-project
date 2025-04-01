WITH
l AS (
    SELECT
        *
    FROM
        AIRBNB.DEV.dim_listings_cleansed
),
h AS (
    SELECT * 
    FROM AIRBNB.DEV.dim_owners_cleansed
)

SELECT
    l.listing_sk,
    l.listing_id,
    l.hotel_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_sk,
    h.owner_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, h.updated_at) as updated_at
FROM l
LEFT JOIN h ON (h.host_id = l.host_id)