{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_key',
        cluster_by=[
            'last_name',
            'first_name',
            'email_address'
        ],
        on_schema_change='sync_all_columns',
<<<<<<< HEAD
        tags=['silver', 'customer']
=======
        tags=[
            'adworks',
            'dimension'
        ]
>>>>>>> dev
    )
}}

WITH source AS (
    SELECT
        *
    FROM 
        {{ ref('AdventureWorksCustomerLookup') }} -- Assuming this is your seed file name
    
    {% if is_incremental() %}
    WHERE
        /* Use appropriate incremental logic based on your update pattern */
        CustomerKey NOT IN (
            SELECT customer_key 
            FROM {{ this }}
        )
    {% endif %}
),

transformed_source AS (
    SELECT
        -- Primary key
        CAST(CustomerKey AS INT64) AS customer_key
        
        -- Personal information
        , TRIM(Prefix) AS prefix
        , TRIM(FirstName) AS first_name
        , TRIM(LastName) AS last_name
        , CAST(BirthDate AS DATE) AS birth_date
        , TRIM(MaritalStatus) AS marital_status
        , TRIM(Gender) AS gender
        , TRIM(EmailAddress) AS email_address
        
        -- Demographic information
        , CAST(AnnualIncome AS FLOAT64) AS annual_income
        , CAST(TotalChildren AS INT64) AS total_children
        , TRIM(EducationLevel) AS education_level
        , TRIM(Occupation) AS occupation
        , CASE WHEN UPPER(HomeOwner) = 'Y' THEN TRUE ELSE FALSE END AS is_home_owner
        
        -- Metadata
        , {{ var('current_timestamp') }} AS _extracted_at
    FROM 
        source
)

SELECT 
    * 
FROM 
    transformed_source