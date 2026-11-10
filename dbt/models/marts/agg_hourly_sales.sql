-- Sales KPIs per hour the orders were placed, and per country.
select
    date_trunc('hour', placed_at)                                         as order_hour,
    country,
    count(*)                                                              as orders_placed,
    count(*) filter (where current_status = 'cancelled')                  as orders_cancelled,
    count(*) filter (where current_status = 'refunded')                   as orders_refunded,
    round(sum(amount_eur) filter (where paid_at is not null), 2)          as gross_revenue_eur,
    round(coalesce(sum(amount_eur) filter (where current_status = 'refunded'), 0), 2)
                                                                          as refunded_eur,
    round(avg(amount_eur), 2)                                             as avg_order_value_eur
from {{ ref('fct_orders') }}
where placed_at is not null
group by 1, 2
