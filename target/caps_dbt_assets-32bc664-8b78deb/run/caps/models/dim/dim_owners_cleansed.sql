
  create or replace   view AIRBNB.DEV.dim_owners_cleansed
  
   as (
    

WITH  __dbt__cte__src_hosts as (
WITH raw_hosts AS (
    SELECT
        *
    FROM
       AIRBNB.RAW.RAW_HOSTS
)
SELECT
    id AS host_id,
    NAME AS owner_name,
    is_superhost,
    cast(created_at as date) AS created_at,
    cast(updated_at as date) AS updated_at
FROM
    raw_hosts
), src_hosts AS (
    SELECT
        *
    FROM
        __dbt__cte__src_hosts
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

