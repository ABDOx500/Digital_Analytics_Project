/*
  This model calculates total revenue for each brand and identifies the top 10.
*/

{{ config(materialized='table') }}

SELECT
    brand,
    SUM(CASE WHEN is_purchase = true THEN price ELSE 0 END) AS total_revenue
FROM
    {{ ref('Star_Schema') }}
WHERE
    brand IS NOT NULL
GROUP BY
    brand
ORDER BY
    total_revenue DESC
LIMIT 10
