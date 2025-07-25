/*
  This model calculates key sales metrics for each brand.
  It identifies the top 10 brands by sales and also includes
  total purchases and average order value (AOV) for each.
*/

{{ config(materialized='table') }}

WITH purchase_events AS (
  SELECT
    brand,
    price,
    -- Use the event_sk as a proxy for a unique order line item
    event_sk
  FROM
    {{ ref('Star_Schema') }}
  WHERE
    is_purchase = true
)

SELECT
    brand,
    SUM(price) AS total_revenue,
    COUNT(DISTINCT event_sk) AS total_purchases,
    SAFE_DIVIDE(SUM(price), COUNT(DISTINCT event_sk)) AS average_order_value
FROM
    purchase_events
GROUP BY
    brand
ORDER BY
    total_revenue DESC
LIMIT 10
