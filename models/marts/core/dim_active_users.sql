with active_users as (
    select * 
    from {{ ref('dim_users') }}
    where DATE_DIFF(CURRENT_DATE(), DATE(last_access_at), DAY) <= 90
)

select * from active_users