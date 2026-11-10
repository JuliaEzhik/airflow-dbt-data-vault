{{ config(materialized='incremental') }}

with base as (
  select
    md5(cast(o.order_id as text)) as hk_order_id,
    md5(cast(o.customer_id as text)) as hk_customer_id,
    o.order_ts as load_ts
  from {{ ref('stg_orders') }} o
),
candidates as (
  select
    md5(hk_order_id || '-' || hk_customer_id) as hk_link_order_customer_id,
    hk_order_id,
    hk_customer_id,
    load_ts
  from base
),
to_insert as (
  select c.*
  from candidates c
  {% if is_incremental() %}
  left join {{ this }} l on l.hk_link_order_customer_id = c.hk_link_order_customer_id
  where l.hk_link_order_customer_id is null
  {% endif %}
)
select * from to_insert

