{{ config(materialized='incremental') }}

with candidates as (
  select distinct
    md5(cast(customer_id as text)) as hk_customer_id,
    customer_id,
    min(order_ts) over (partition by customer_id) as load_ts
  from {{ ref('stg_orders') }}
),
to_insert as (
  select c.*
  from candidates c
  {% if is_incremental() %}
  left join {{ this }} h on h.hk_customer_id = c.hk_customer_id
  where h.hk_customer_id is null
  {% endif %}
)
select * from to_insert

