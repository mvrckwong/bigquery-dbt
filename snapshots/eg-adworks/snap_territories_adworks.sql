{% snapshot snap_territories_adworks %}

{{
    config(
        unique_key='sales_territory_key',
        strategy='check',
        check_cols=[
            'region',
            'country',
            'continent'
        ],
        invalidate_hard_deletes=True,
        tags=['gold', 'territory']
    )
}}

SELECT 
    * 
FROM 
    {{ ref('stg_territory_adworks') }}

{% endsnapshot %}