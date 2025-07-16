/*
  This model calculates the number of daily active users (DAU).
  An active user is defined as any user with at least one event on a given day.
*/

{{ config(materialized='table') }}

SELECT
    event_date,
    COUNT(DISTINCT user_name) AS daily_active_users
FROM
    {{ ref('Star_Schema') }}
GROUP BY
    event_date
ORDER BY
    event_date DESC
