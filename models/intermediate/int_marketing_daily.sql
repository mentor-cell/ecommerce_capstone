with
    cleaned as (
        select date, canonical_channel, campaign_id, impressions, clicks, cost_usd
        from {{ ref("stg_ad_spend") }}
    )
select
    date,
    canonical_channel,
    campaign_id,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(cost_usd) as cost_usd,
    safe_divide(sum(clicks), nullif(sum(impressions), 0)) as ctr,
    safe_divide(sum(cost_usd), nullif(sum(clicks), 0)) as cpc,
    safe_divide(1000 * sum(cost_usd), nullif(sum(impressions), 0)) as cpm
from cleaned
group by 1, 2, 3
