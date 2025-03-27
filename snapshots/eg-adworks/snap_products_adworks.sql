{% snapshot snap_products_adworks %}

{{
    config(
        unique_key='ProductKey',
        strategy='check',
        check_cols=[
            'ProductName',
            'ModelName',
            'ProductDescription',
            'ProductColor',
            'ProductSize',
            'ProductStyle',
            'ProductCost',
            'ProductPrice'
        ],
        invalidate_hard_deletes=True,
        tags=['eg'],
        enabled=false
    )
}}

SELECT 
    * 
FROM 
    {{ ref('stg_products_adworks') }}

{% endsnapshot %}