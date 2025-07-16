/*
  This model is an intermediate step that joins the fact table 
  with all dimension tables to create a single, wide, and descriptive 
  table of all user events. It will be the foundation for all final KPI models.
*/

{{ config(materialized='table') }}

-- CTE to uniquely number each event within a group of identical events
WITH numbered_events AS (
    SELECT
        f.*, -- Select all columns from the fact table
        s.session_start,
        s.duration_s,
        ROW_NUMBER() OVER(
            PARTITION BY 
                f.session_sk, 
                f.user_sk, 
                f.product_sk, 
                f.event_type_sk, 
                s.session_start 
            ORDER BY 
                f.session_sk
        ) as event_occurrence
    FROM
        {{ source('ecommerce_data', 'fact_events') }} AS f
    LEFT JOIN 
        {{ source('ecommerce_data', 'dim_session') }} AS s 
        ON f.session_sk = s.session_sk
)
-- Final model joining all dimensions
SELECT
    -- Generate a guaranteed unique key by including the event_occurrence
    {{ dbt_utils.generate_surrogate_key([
        'ne.session_sk',
        'ne.user_sk',
        'ne.product_sk',
        'ne.event_type_sk',
        'ne.session_start',
        'ne.event_occurrence' 
    ]) }} as event_sk,

    -- Columns from fact_events
    ne.price,
    ne.is_purchase,

    -- Columns from dim_date
    d.date as event_date,
    d.day_of_week,

    -- Columns from dim_event_type
    et.event_type,

    -- Columns from dim_product
    p.brand,
    p.category_code,
    
    -- Columns from dim_user
    u.user_name,

    -- Columns from dim_session
    ne.session_start,
    ne.duration_s as session_duration_seconds

FROM
    numbered_events AS ne
LEFT JOIN {{ source('ecommerce_data', 'dim_date') }} AS d ON ne.date_sk = d.date_sk
LEFT JOIN {{ source('ecommerce_data', 'dim_event_type') }} AS et ON ne.event_type_sk = et.event_type_sk
LEFT JOIN {{ source('ecommerce_data', 'dim_product') }} AS p ON ne.product_sk = p.product_sk
LEFT JOIN {{ source('ecommerce_data', 'dim_user') }} AS u ON ne.user_sk = u.user_sk
-- No need to join dim_session again as it's in the CTE