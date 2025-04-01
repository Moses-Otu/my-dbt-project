{{ config(
  materialized = 'table'
) }}

WITH fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),
holiday_dates AS (
    SELECT DISTINCT Date AS holiday_date,holiday FROM {{ ref('festive_holidays_past_10_years') }}
)

SELECT 
    r.listing_id,
    r.review_date,
    r.reviewer_name,
    r.review_text,
    r.review_sentiment,
    CASE 
        WHEN d.holiday_date IS NOT NULL THEN 'Y'
        ELSE 'N'
    END AS is_holiday,
    d.holiday
FROM fct_reviews r
LEFT JOIN holiday_dates d 
    ON r.review_date = d.holiday_date
