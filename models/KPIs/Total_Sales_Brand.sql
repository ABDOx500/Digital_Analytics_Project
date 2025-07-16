/*
  This is a final KPI model. It calculates total sales for each brand.
  It reads from the pre-joined 'Star_Schema'.
*/

{{ config(materialized='table') }}

SELECT
    brand,
    SUM(price) as total_sales,
    COUNT(event_sk) as total_purchases
FROM
    -- This ref() function is the link to your first model!
    {{ ref('Star_Schema') }}
WHERE
    is_purchase = true
GROUP BY
    brand
ORDER BY
    total_sales DESC