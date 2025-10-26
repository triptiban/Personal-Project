{{ config(materialized='view') }}
select
  u.user_id,
  lower(u.email) as email,
  nullif(trim(u.username),'') as username,
  nullif(trim(u.full_name),'') as full_name,
  nullif(trim(u.city),'') as city,
  u._ingested_at
from raw.users u