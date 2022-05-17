with final as (
    select * from {{ ref('int_mart_all_users') }}
)

select * from final
