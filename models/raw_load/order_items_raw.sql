select * from {{ ref('order_items_batch1') }}
UNION ALL
select * from {{ ref('order_items_batch2') }}

