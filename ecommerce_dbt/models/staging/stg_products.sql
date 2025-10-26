{{ config(materialized='view') }}
select
  p.product_id,
  nullif(trim(p.title),'') as title,
  cast(p.price as numeric(10,2)) as price,
  nullif(trim(p.category),'') as category,
  p._ingested_at
from raw.products p
