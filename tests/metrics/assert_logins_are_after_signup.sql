with users as(
    select * from {{ ref('users') }}
),

logins as (
    select * from {{ ref('user_logins') }}
),

final as(
    select
        logins.user_id,
        users.registered_at,
        min(logins.login_at) as min_login_at

    from logins
    left join users
        on logins.user_id = users.user_id

    where login_at < registered_at
    group by 1,2
)

select * from final