-- One row per order with the timestamp of every lifecycle step.
select
    e.order_id,
    o.customer_id,
    o.current_status,
    o.amount,
    o.currency,
    o.country,
    o.payment_method,
    min(case when e.status = 'placed'    then e.event_ts end) as placed_at,
    min(case when e.status = 'paid'      then e.event_ts end) as paid_at,
    min(case when e.status = 'shipped'   then e.event_ts end) as shipped_at,
    min(case when e.status = 'delivered' then e.event_ts end) as delivered_at,
    min(case when e.status = 'cancelled' then e.event_ts end) as cancelled_at,
    min(case when e.status = 'refunded'  then e.event_ts end) as refunded_at,
    max(e.loaded_at)                                          as last_loaded_at
from {{ source('staging', 'order_events') }} as e
join {{ source('staging', 'orders') }} as o using (order_id)
group by 1, 2, 3, 4, 5, 6, 7
