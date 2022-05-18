with user_history as (
    select * from {{ ref('stg_clean_snapshot_users') }}
),

all_users as (
    select user_id from {{ ref('all_users') }}
),

final as (
    select
        distinct user_history.user_id

    from user_history
    left join all_users
        on user_history.user_id = all_users.user_id
    where all_users.user_id is null
)

select * from final
