with
    base as (
        select
            order_id,
            product_sku,
            safe_cast(
                regexp_replace(trim(cast(qty_text as string)), r'[^0-9]', '') as int64
            ) as qty,
            {{ parse_money("unit_price_text") }} as unit_price_num
        from {{ ref("order_items_raw") }}
    )
select
    order_id,
    product_sku,
    coalesce(qty, 0) as qty,
    unit_price_num,
    round(coalesce(qty, 0) * unit_price_num, 2) as line_revenue_usd
from base
