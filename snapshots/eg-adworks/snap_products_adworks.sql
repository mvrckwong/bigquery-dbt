{% snapshot snap_products_adworks %}

{{
    config(
        unique_key='product_key',
        strategy='check',
        check_cols=[
            'product_name',
            'product_sku',
            'model_name',
            'product_description',
            'product_color',
            'product_size',
            'product_style',
            'product_cost',
            'product_price'
        ],
        invalidate_hard_deletes=True,
        tags=['gold', 'product']
    )
}}

SELECT 
    * 
FROM 
    {{ ref('stg_products_adworks') }}

{% endsnapshot %}