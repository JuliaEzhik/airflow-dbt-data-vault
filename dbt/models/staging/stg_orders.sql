select
  order_id,
  order_ts,
  customer_id,
  order_amount,
  status,
  updated_at
from {{ source('raw', 'orders') }}

