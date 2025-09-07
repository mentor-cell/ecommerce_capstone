{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["date", "canonical_channel", "campaign_id"],
        partition_by={"field": "date", "data_type": "date"},
        on_schema_change="sync_all_columns",
        tags=["daily_incremental", "weekly_refresh"],
    )
}}

select
    m.date,
    m.canonical_channel,
    m.campaign_id,
    m.impressions,
    m.clicks,
    m.cost_usd,
    ord.revenue_usd,
    safe_divide(ord.revenue_usd, nullif(m.cost_usd, 0)) as roas
from {{ ref("int_marketing_daily") }} m
left join
    (
        select
            date(order_ts_utc) as date,
            utm_source as canonical_channel,  -- or map more carefully if needed
            utm_campaign as campaign_id,
            sum(order_total_usd) as revenue_usd
        from {{ ref("int_orders_enriched") }}
        where is_paid
        group by 1, 2, 3
    ) ord using (date, canonical_channel, campaign_id)
{% if is_incremental() %}
    where m.date >= (select date_sub(max(date), interval 30 day) from {{ this }})
{% endif %}
