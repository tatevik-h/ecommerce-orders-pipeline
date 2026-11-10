-- Business rule: an order's lifecycle steps must happen in order.
-- Returns the orders that break it; the test passes when no rows come back.
select order_id, placed_at, paid_at, shipped_at, delivered_at, refunded_at
from {{ ref('fct_orders') }}
where paid_at      < placed_at
   or shipped_at   < paid_at
   or delivered_at < shipped_at
   or refunded_at  < delivered_at
