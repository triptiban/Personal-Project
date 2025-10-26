{{ config(
    materialized='incremental',
    unique_key=['cart_id','product_id']
) }}

with base as (
  select
    c.cart_id,
    c.user_id as customer_id,
    c.order_date,
    i.product_id,
    i.quantity,
    i.line_amount
  from {{ ref('stg_carts') }} c
  join {{ ref('int_order_items') }} i on i.cart_id = c.cart_id
  {% if is_incremental() %}
    where c.order_date >= (
      select coalesce(max(order_date), date '1900-01-01')
      from {{ this }}
    )
  {% endif %}
)

select * from base
