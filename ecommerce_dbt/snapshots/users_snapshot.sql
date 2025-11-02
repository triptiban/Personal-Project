{% snapshot users_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=['username', 'full_name', 'email', 'city']
    )
  }}

  select
    user_id,
    username,
    full_name,
    email,
    city
  from {{ ref('stg_users') }}
{% endsnapshot %}
