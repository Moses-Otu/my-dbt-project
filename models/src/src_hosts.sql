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