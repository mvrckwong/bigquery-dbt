{{
    config(
        materialized='table',
        schema='gold',
        tags=[
            'general',
            'dimension'
        ]
    )
}}

-- Define date range variables for flexibility
{% set start_date = "DATE('2000-01-01')" %}
{% set end_date = "DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR), INTERVAL 1 DAY)" %}

WITH date_range AS (
    SELECT calendar_date
    FROM UNNEST(GENERATE_DATE_ARRAY(
        {% if is_incremental() %}
            -- If incremental, only generate dates we don't have yet
            CAST(
                (SELECT DATE_ADD(MAX(calendar_date), INTERVAL 1 DAY) FROM {{ this }}) AS DATE
            )
        {% else %}
            -- Initial load starts from 2000
            {{ start_date }}
        {% endif %},
        {{ end_date }},
        INTERVAL 1 DAY
    )) AS calendar_date
),
enriched_date AS (
    SELECT
        d.calendar_date
        
        -- Date parts extraction
        , EXTRACT(YEAR FROM d.calendar_date) AS calendar_year
        , EXTRACT(MONTH FROM d.calendar_date) AS calendar_month
        , EXTRACT(DAY FROM d.calendar_date) AS calendar_day
        , FORMAT_DATE('%B', d.calendar_date) AS month_name
        , FORMAT_DATE('%b', d.calendar_date) AS month_name_short
        , EXTRACT(QUARTER FROM d.calendar_date) AS calendar_quarter
        , FORMAT_DATE('Q%Q', d.calendar_date) AS quarter_name
        , EXTRACT(WEEK FROM d.calendar_date) AS calendar_week
        
        -- Week info extraction
        , EXTRACT(DAYOFWEEK FROM d.calendar_date) AS day_of_week
        , FORMAT_DATE('%A', d.calendar_date) AS day_name
        , FORMAT_DATE('%a', d.calendar_date) AS day_name_short
        , CASE 
            WHEN EXTRACT(DAYOFWEEK FROM d.calendar_date) = 1 THEN 7
            ELSE EXTRACT(DAYOFWEEK FROM d.calendar_date) - 1
          END AS day_of_week_iso
        
        -- Month start/end calculations
        , DATE_TRUNC(d.calendar_date, MONTH) AS first_day_of_month
        , LAST_DAY(d.calendar_date) AS last_day_of_month
        , DATE_DIFF(LAST_DAY(d.calendar_date), DATE_TRUNC(d.calendar_date, MONTH), DAY) + 1 AS days_in_month
        
        -- Quarter start/end calculations
        , DATE_TRUNC(d.calendar_date, QUARTER) AS first_day_of_quarter
        , LAST_DAY(DATE_TRUNC(d.calendar_date, QUARTER) + INTERVAL 2 MONTH) AS last_day_of_quarter
        
        -- Year start/end calculations
        , DATE(EXTRACT(YEAR FROM d.calendar_date), 1, 1) AS first_day_of_year
        , DATE(EXTRACT(YEAR FROM d.calendar_date), 12, 31) AS last_day_of_year
        
        -- Additional flags for business needs
        , CASE 
            WHEN EXTRACT(DAYOFWEEK FROM d.calendar_date) IN (1, 7) THEN TRUE 
            ELSE FALSE 
          END AS is_weekend
        , CASE 
            WHEN FORMAT_DATE('%B %d', d.calendar_date) = 'January 01' THEN TRUE
            WHEN FORMAT_DATE('%B %d', d.calendar_date) = 'July 04' THEN TRUE
            WHEN FORMAT_DATE('%B %d', d.calendar_date) = 'December 25' THEN TRUE
            -- Add other holidays as needed
            ELSE FALSE
          END AS is_holiday
        , CASE 
            WHEN EXTRACT(DAY FROM LAST_DAY(d.calendar_date)) = EXTRACT(DAY FROM d.calendar_date) THEN TRUE
            ELSE FALSE
          END AS is_last_day_of_month
        
        -- Sort order fields
        , CASE 
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Monday' THEN 1
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Tuesday' THEN 2
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Wednesday' THEN 3
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Thursday' THEN 4
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Friday' THEN 5
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Saturday' THEN 6
            WHEN FORMAT_DATE('%A', d.calendar_date) = 'Sunday' THEN 7
          END AS day_name_sort
        , EXTRACT(MONTH FROM d.calendar_date) AS month_sort
        , EXTRACT(QUARTER FROM d.calendar_date) AS quarter_sort
        
        -- Fiscal year calculations (assuming April-March fiscal year)
        , EXTRACT(YEAR FROM d.calendar_date) + CASE 
            WHEN EXTRACT(MONTH FROM d.calendar_date) >= 4 THEN 0 
            ELSE -1 
          END AS fiscal_year
        , CASE 
            WHEN EXTRACT(MONTH FROM d.calendar_date) >= 4 THEN EXTRACT(MONTH FROM d.calendar_date) - 3
            ELSE EXTRACT(MONTH FROM d.calendar_date) + 9
          END AS fiscal_month
        , CASE 
            WHEN EXTRACT(MONTH FROM d.calendar_date) >= 4 THEN CEILING((EXTRACT(MONTH FROM d.calendar_date) - 3) / 3)
            ELSE CEILING((EXTRACT(MONTH FROM d.calendar_date) + 9) / 3)
          END AS fiscal_quarter
    FROM 
        date_range d
)

SELECT
    *
FROM
    enriched_date
ORDER BY
    calendar_date