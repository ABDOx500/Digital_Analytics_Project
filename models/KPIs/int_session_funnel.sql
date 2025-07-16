/*
  This intermediate model summarizes each user session.
  It creates boolean flags to identify if a session included key funnel steps:
  viewing a product, adding to cart, or making a purchase.
*/

{{ config(materialized='table') }}

SELECT
    session_start,
    
    -- Create a boolean flag (true/false) for each step of the funnel.
    MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) = 1 AS did_view,
    MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) = 1 AS did_add_to_cart,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) = 1 AS did_purchase

FROM
    {{ ref('Star_Schema') }}
GROUP BY
    session_start
