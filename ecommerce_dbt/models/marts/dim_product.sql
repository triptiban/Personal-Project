{{ config(materialized='table') }}
select
  product_id,
  title,
  category,
  price
from {{ ref('stg_products') }}
