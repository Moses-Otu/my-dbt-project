{{
  config(
    materialized = 'view',
    unique_key = 'host_sk',
    )
}}

WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('src_hosts') }}
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