{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

-- Order fact table. Incremental: each run only re-processes orders that
-- received new or updated events since the last build, then merges them in.
select
    l.order_id,
    l.customer_id,
    l.current_status,
    l.amount,
    l.currency,
    round(l.amount * fx.rate_to_eur, 2)                                    as amount_eur,
    l.country,
    l.payment_method,
    l.placed_at,
    l.paid_at,
    l.shipped_at,
    l.delivered_at,
    l.cancelled_at,
    l.refunded_at,
    round(extract(epoch from (l.shipped_at - l.paid_at)) / 3600.0, 2)      as hours_to_ship,
    round(extract(epoch from (l.delivered_at - l.shipped_at)) / 3600.0, 2) as hours_in_transit,
    l.last_loaded_at
from {{ ref('int_order_lifecycle') }} as l
join {{ ref('fx_rates') }} as fx using (currency)
{% if is_incremental() %}
where l.last_loaded_at > (select coalesce(max(last_loaded_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
