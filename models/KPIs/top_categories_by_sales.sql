/*
  This model calculates total revenue for each product category and identifies the top 10.
*/

{{ config(materialized='table') }}

SELECT
    category_code,
    SUM(CASE WHEN is_purchase = true THEN price ELSE 0 END) AS total_revenue
FROM
    {{ ref('Star_Schema') }}
WHERE
    category_code IS NOT NULL
GROUP BY
    category_code
ORDER BY
    total_revenue DESC
LIMIT 10
