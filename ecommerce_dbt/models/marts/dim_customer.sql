{{ config(materialized='table') }}
select
  user_id as customer_id,
  full_name,
  email,
  city
from {{ ref('int_customers') }}
