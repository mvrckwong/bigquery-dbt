{{
	config(
		materialized='table',
		dataset='silver'
	) 
}}

WITH source AS (
      SELECT
		customer_id
		, customer_acct_num
		, first_name
		, last_name
		, customer_address
		, customer_city
		, customer_state_province
		, customer_postal_code
		, customer_country
		, birthdate AS customer_birthdate
		, marital_status AS customer_marital_status
		, yearly_income AS customer_yearly_income
		, gender AS customer_gender
		, total_children AS customer_total_children
		, num_children_at_home AS customer_num_children_at_home
		, education AS customer_education
		, acct_open_date AS customer_acct_open_date
		, member_card AS customer_member_card
		, occupation AS customer_occupation
		, homeowner AS is_customer_homeowner
      FROM 
            {{ ref('raw_seed_customers') }}
)
SELECT
	*
FROM 
      source