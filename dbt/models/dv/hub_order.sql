{{ config(materialized='incremental') }}

with candidates as (
  select distinct
    md5(cast(order_id as text)) as hk_order_id,
    order_id,
    min(order_ts) over (partition by order_id) as load_ts
  from {{ ref('stg_orders') }}
),
to_insert as (
  select c.*
  from candidates c
  {% if is_incremental() %}
  left join {{ this }} h on h.hk_order_id = c.hk_order_id
  where h.hk_order_id is null
  {% endif %}
)
select * from to_insert

