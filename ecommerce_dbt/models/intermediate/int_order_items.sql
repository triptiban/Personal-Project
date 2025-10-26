{{ config(materialized='view') }}
select
  i.cart_id,
  i.product_id,
  i.quantity,
  p.price,
  (i.quantity * p.price) as line_amount
from {{ ref('stg_cart_items') }} i
join {{ ref('stg_products') }} p
  on p.product_id = i.product_id
