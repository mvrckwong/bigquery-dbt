{% snapshot snap_customers_adworks %}

{{
    config(
        unique_key='customer_key',
        strategy='check',
        check_cols=[
            'prefix',
            'first_name',
            'last_name',
            'birth_date',
            'marital_status',
            'gender',
            'email_address',
            'annual_income',
            'total_children',
            'education_level',
            'occupation',
            'is_home_owner'
        ],
        invalidate_hard_deletes=True,
        tags=['gold', 'customer']
    )
}}

SELECT 
    * 
FROM 
    {{ ref('stg_customers_adworks') }}

{% endsnapshot %}