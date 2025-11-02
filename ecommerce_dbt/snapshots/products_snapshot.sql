{% snapshot products_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='check',
      check_cols=['title', 'category', 'price']
    )
  }}

  select
    product_id,
    title,
    category,
    price
  from {{ ref('stg_products') }}
{% endsnapshot %}
