with base as (
  select
    DATE(parse_date('%Y-%m-%d', CAST(date AS STRING)))        as date,
    LOWER(TRIM(CAST(channel AS STRING)))                      as raw_channel,
    TRIM(CAST(campaign_id AS STRING))                         as campaign_id,
    SAFE_CAST(TRIM(CAST(impressions AS STRING)) AS INT64)     as impressions,
    SAFE_CAST(TRIM(CAST(clicks AS STRING)) AS INT64)          as clicks,
    {{ parse_money('cost_text') }}                            as cost_usd
  from {{ ref('ad_spend_raw') }}
),
mapped as (
  select
    b.*,
    m.canonical_channel,
    COALESCE(m.paid_flag, false) as paid_flag
  from base b
  left join {{ ref('seed_channel_map') }} m
    on b.raw_channel = m.raw_channel
)
select * from mapped
