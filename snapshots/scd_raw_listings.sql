{% snapshot scd_raw_listings %}

{{
   config(
       target_schema='DEV',
       unique_key='listing_id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}


SELECT 
    listing_id,hotel_name,hotel_url,room_type,minimum_nights,host_id,price,created_at,
    updated_at::TIMESTAMP_NTZ AS updated_at  -- Ensure it's the correct datatype
FROM {{ ref("src_listings") }}

{% endsnapshot %}
-- This snapshot will track changes to the src_listings table based on the updated_at column.
-- It will create a new record in the snapshot table whenever there is a change in the listing_id or updated_at column.