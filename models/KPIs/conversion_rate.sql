/*
  This model calculates high-level funnel conversion rates by aggregating
  the session summaries from the int_session_funnel model.
  The output is a single row with all key conversion metrics.
*/

{{ config(materialized='table') }}

WITH session_summary AS (
    SELECT
        -- Count the number of sessions that hit each stage of the funnel
        COUNTIF(did_view) AS total_sessions_with_view,
        COUNTIF(did_add_to_cart) AS total_sessions_with_cart,
        COUNTIF(did_purchase) AS total_sessions_with_purchase
    FROM
        {{ ref('int_session_funnel') }}
)

SELECT
    -- Funnel Step 1: View -> Cart
    SAFE_DIVIDE(total_sessions_with_cart, total_sessions_with_view) AS view_to_cart_rate,

    -- Funnel Step 2: Cart -> Purchase
    SAFE_DIVIDE(total_sessions_with_purchase, total_sessions_with_cart) AS cart_to_purchase_rate,

    -- Overall Funnel: View -> Purchase
    SAFE_DIVIDE(total_sessions_with_purchase, total_sessions_with_view) AS view_to_purchase_conversion_rate

FROM
    session_summary
