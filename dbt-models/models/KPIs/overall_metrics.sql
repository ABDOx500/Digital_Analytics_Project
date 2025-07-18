/*
  This model calculates site-wide business metrics like Average Order Value (AOV)
  and Cart Abandonment Rate. The output is a single row.
*/

{{ config(materialized='table') }}

WITH session_summary AS (
    SELECT
        -- Use the intermediate funnel model to get session behavior
        *
    FROM
        {{ ref('int_session_funnel') }}
),

purchase_revenue AS (
    -- Calculate total revenue only from purchase events
    SELECT
        SUM(price) as total_revenue
    FROM
        {{ ref('Star_Schema') }}
    WHERE
        is_purchase = true
)

SELECT
    -- Calculate Average Order Value (AOV)
    SAFE_DIVIDE(
        (SELECT total_revenue FROM purchase_revenue),
        (SELECT COUNTIF(did_purchase) FROM session_summary)
    ) AS average_order_value,

    -- Calculate Cart Abandonment Rate
    SAFE_DIVIDE(
        (SELECT COUNTIF(did_add_to_cart AND NOT did_purchase) FROM session_summary),
        (SELECT COUNTIF(did_add_to_cart) FROM session_summary)
    ) AS cart_abandonment_rate

