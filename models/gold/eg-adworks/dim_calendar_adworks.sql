{{
    config(
        materialized='table',
        unique_key='calendar_key',
        partition_by={
            "field": "calendar_date"
            , "data_type": "date"
            , "granularity": "month"
        },
        cluster_by=[
            'calendar_year',
            'calendar_month',
            'day_of_week'
        ],
        tags=['dimension', 'calendar']
    )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.star(from=ref('stg_calendar_adworks')) }}
    FROM 
        {{ ref('stg_calendar_adworks') }}
),

-- Enriched data with additional calendar attributes and extractions
enriched AS (
    SELECT
        s.*
        
        -- Date parts extraction
        , EXTRACT(YEAR FROM s.calendar_date) AS calendar_year
        , EXTRACT(MONTH FROM s.calendar_date) AS calendar_month
        , EXTRACT(DAY FROM s.calendar_date) AS calendar_day
        , FORMAT_DATE('%B', s.calendar_date) AS month_name
        , FORMAT_DATE('%b', s.calendar_date) AS month_name_short
        , EXTRACT(QUARTER FROM s.calendar_date) AS calendar_quarter
        , FORMAT_DATE('Q%Q', s.calendar_date) AS quarter_name
        , EXTRACT(WEEK FROM s.calendar_date) AS calendar_week
        
        -- Week info extraction
        , EXTRACT(DAYOFWEEK FROM s.calendar_date) AS day_of_week
        , FORMAT_DATE('%A', s.calendar_date) AS day_name
        , FORMAT_DATE('%a', s.calendar_date) AS day_name_short
        , CASE 
            WHEN EXTRACT(DAYOFWEEK FROM s.calendar_date) = 1 THEN 7
            ELSE EXTRACT(DAYOFWEEK FROM s.calendar_date) - 1
          END AS day_of_week_iso
        
        -- Month start/end calculations
        , DATE_TRUNC(s.calendar_date, MONTH) AS first_day_of_month
        , LAST_DAY(s.calendar_date) AS last_day_of_month
        , DATE_DIFF(LAST_DAY(s.calendar_date), DATE_TRUNC(s.calendar_date, MONTH), DAY) + 1 AS days_in_month
        
        -- Quarter start/end calculations
        , DATE_TRUNC(s.calendar_date, QUARTER) AS first_day_of_quarter
        , LAST_DAY(DATE_TRUNC(s.calendar_date, QUARTER) + INTERVAL 2 MONTH) AS last_day_of_quarter
        
        -- Year start/end calculations
        , DATE(EXTRACT(YEAR FROM s.calendar_date), 1, 1) AS first_day_of_year
        , DATE(EXTRACT(YEAR FROM s.calendar_date), 12, 31) AS last_day_of_year
        
        -- Additional flags for business needs
        , CASE 
            WHEN EXTRACT(DAYOFWEEK FROM s.calendar_date) IN (1, 7) THEN TRUE 
            ELSE FALSE 
          END AS is_weekend
        , CASE 
            WHEN FORMAT_DATE('%B %d', s.calendar_date) = 'January 01' THEN TRUE
            WHEN FORMAT_DATE('%B %d', s.calendar_date) = 'July 04' THEN TRUE
            WHEN FORMAT_DATE('%B %d', s.calendar_date) = 'December 25' THEN TRUE
            -- Add other holidays as needed
            ELSE FALSE
          END AS is_holiday
        , CASE 
            WHEN EXTRACT(DAY FROM LAST_DAY(s.calendar_date)) = EXTRACT(DAY FROM s.calendar_date) THEN TRUE
            ELSE FALSE
          END AS is_last_day_of_month
        
        -- Sort order fields
        , CASE 
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Monday' THEN 1
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Tuesday' THEN 2
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Wednesday' THEN 3
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Thursday' THEN 4
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Friday' THEN 5
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Saturday' THEN 6
            WHEN FORMAT_DATE('%A', s.calendar_date) = 'Sunday' THEN 7
          END AS day_name_sort
        , EXTRACT(MONTH FROM s.calendar_date) AS month_sort
        , EXTRACT(QUARTER FROM s.calendar_date) AS quarter_sort
        
        -- Fiscal year calculations
        , EXTRACT(YEAR FROM s.calendar_date) + CASE 
            WHEN EXTRACT(MONTH FROM s.calendar_date) >= 4 THEN 0 
            ELSE -1 
          END AS fiscal_year
        , CASE 
            WHEN EXTRACT(MONTH FROM s.calendar_date) >= 4 THEN EXTRACT(MONTH FROM s.calendar_date) - 3
            ELSE EXTRACT(MONTH FROM s.calendar_date) + 9
          END AS fiscal_month
        , CASE 
            WHEN EXTRACT(MONTH FROM s.calendar_date) >= 4 THEN CEILING((EXTRACT(MONTH FROM s.calendar_date) - 3) / 3)
            ELSE CEILING((EXTRACT(MONTH FROM s.calendar_date) + 9) / 3)
          END AS fiscal_quarter
    FROM 
        source s
),

-- Final dimension table with restructured columns
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['e.date_id']) }} AS calendar_key
        
        -- Original date identifiers
        , e.date_id AS source_date_id
        , e.calendar_date
        
        -- Year, quarter, month, day hierarchy
        , e.calendar_year
        , e.calendar_quarter
        , e.quarter_name
        , e.calendar_month
        , e.month_name
        , e.month_name_short
        , e.calendar_day
        
        -- Week information
        , e.calendar_week
        , e.day_of_week
        , e.day_of_week_iso
        , e.day_name
        , e.day_name_short
        
        -- Period start/end dates
        , e.first_day_of_month
        , e.last_day_of_month
        , e.first_day_of_quarter
        , e.last_day_of_quarter
        , e.first_day_of_year
        , e.last_day_of_year
        
        -- Business flags
        , e.is_weekend
        , e.is_holiday
        , e.is_last_day_of_month
        
        -- Fiscal calendar
        , e.fiscal_year
        , e.fiscal_month
        , e.fiscal_quarter
        
        -- Sort orders
        , e.day_name_sort
        , e.month_sort
        , e.quarter_sort
        , e.days_in_month
        
        -- Relative dates (for filtering)
        , DATE_DIFF(CURRENT_DATE(), e.calendar_date, DAY) AS days_ago
        , DATE_DIFF(e.calendar_date, CURRENT_DATE(), DAY) AS days_ahead
        
        -- Metadata
        , e._extracted_at
    FROM 
        enriched e
)

SELECT 
    * 
FROM 
    final