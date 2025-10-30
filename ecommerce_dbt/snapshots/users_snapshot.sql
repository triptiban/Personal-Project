{% snapshot users_snapshot %}

{{ config(
  target_schema='snapshots',
  unique_key='user_id',
  strategy='timestamp',
  updated_at='_ingested_at' 
) }}

select
  user_id,
  username,
  full_name,
  email,
  city,
  _ingested_at
from {{ ref('stg_users') }}  

{% endsnapshot %}
