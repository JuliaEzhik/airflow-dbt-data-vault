{{ config(materialized='incremental') }}

with s as (
  select
    md5(cast(order_id as text)) as hk_order_id,
    order_amount,
    status,
    updated_at as load_ts
  from {{ ref('stg_orders') }}
),
to_insert as (
  select s.*
  from s
  {% if is_incremental() %}
  left join (
    select hk_order_id, max(load_ts) as max_load_ts
    from {{ this }}
    group by hk_order_id
  ) e on e.hk_order_id = s.hk_order_id
  where e.max_load_ts is null or s.load_ts > e.max_load_ts
  {% endif %}
)
select * from to_insert

