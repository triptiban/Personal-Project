{% snapshot products_snapshot %}

{{ config(
  target_schema='snapshots',
  unique_key='product_id',
  strategy='timestamp',
  updated_at='_ingested_at'
) }}

select
  product_id,
  title,
  category,
  price,
  _ingested_at
from {{ ref('stg_products') }}

{% endsnapshot %}
