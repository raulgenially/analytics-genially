with billing as(
    select * from {{ ref('billing') }}
),

select
    *
from billing
where invoiced_at >= current_date() - 7