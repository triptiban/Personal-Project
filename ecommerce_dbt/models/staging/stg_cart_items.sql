{{ config(materialized='view') }}
select
  ci.cart_id,
  ci.product_id,
  ci.quantity
from raw.cart_items ci
