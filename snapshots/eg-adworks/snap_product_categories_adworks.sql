{% snapshot snap_product_categories_adworks %}

{{
    config(
        unique_key='product_category_key',
        strategy='check',
        check_cols=[
            'category_name'
        ],
        invalidate_hard_deletes=True,
        tags=['gold', 'product_category']
    )
}}

SELECT 
    * 
FROM 
    {{ ref('stg_product_categories_adworks') }}

{% endsnapshot %}