with facturation as(
    select * from {{ ref('facturation') }}
),

select
    *
from facturation
where invoiced_at >= current_date() - 7