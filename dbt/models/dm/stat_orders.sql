{{ config(
  schema='dm',
  materialized='incremental',
  unique_key='order_date',
  incremental_strategy='merge'
) }}

with agg as (
  select
    date_trunc('day', order_ts)::date as order_date,
    count(*) as orders_count,
    sum(order_amount) as gross_revenue,
    avg(order_amount) as avg_order_value
  from {{ ref('stg_orders') }}
  {% if is_incremental() %}
    and date_trunc('day', order_ts)::date >= (select coalesce(max(order_date), date '1970-01-01') from {{ this }})
  {% endif %}
  group by 1
)
select * from agg