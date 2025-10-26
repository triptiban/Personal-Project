{{ config(materialized='view') }}
select
  u.user_id,
  u.full_name,
  u.email,
  u.city
from {{ ref('stg_users') }} u
