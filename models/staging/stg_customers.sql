select
    customer_id,
    initcap(trim(full_name)) as full_name_clean,
    lower(trim(email)) as email_norm,
    upper(coalesce(trim(country_code), 'US')) as country_code,
    {{ try_to_ts("created_ts") }} as created_ts_utc,
    {{ try_to_ts("updated_at") }} as updated_at
from {{ ref("customers_raw") }}
