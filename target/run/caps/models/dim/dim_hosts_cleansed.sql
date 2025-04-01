
  create or replace   view AIRBNB.DEV.dim_hosts_cleansed
  
   as (
    

WITH src_hosts AS (
    SELECT
        *
    FROM
        AIRBNB.DEV.src_hosts
)
SELECT
    MD5(CONCAT(host_id, created_at)) AS host_sk,
    host_id,
    owner_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts
  );

