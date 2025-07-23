/*
  This model provides a daily summary of key activity metrics.
  It calculates Daily Active Users (DAU) and the total number of purchase events per day.
*/

{{ config(materialized='table') }}

SELECT
    event_date,
    
    -- Calculate Daily Active Users
    COUNT(DISTINCT user_name) AS daily_active_users,
    
    -- Calculate Total Purchases for the day
    COUNTIF(is_purchase = true) AS total_purchases

FROM
    {{ ref('Star_Schema') }}
GROUP BY
    event_date
ORDER BY
    event_date DESC
