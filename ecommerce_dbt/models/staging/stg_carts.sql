{{ config(materialized='view') }}
select
  c.cart_id,
  c.user_id,
  c.date::date as order_date,
  c._ingested_at
from raw.carts c
