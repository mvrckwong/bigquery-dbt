{% snapshot snap_product_subcategories_adworks %}

{{
    config(
        unique_key='product_subcategory_key',
        strategy='check',
        check_cols=[
            'subcategory_name',
            'product_category_key'
        ],
        invalidate_hard_deletes=True,
        tags=['gold', 'product']
    )
}}

SELECT 
    * 
FROM 
    {{ ref('stg_product_subcategories_adworks') }}

{% endsnapshot %}