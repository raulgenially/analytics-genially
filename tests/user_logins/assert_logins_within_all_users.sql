-- ALl logins in user_logins model should be contained in all_users model
with users as(
    select * from {{ ref('all_users') }}
),

logins as (
    select * from {{ ref('user_logins') }}
),

final as(
    select
        logins.*

    from logins
    left join users
        on logins.user_id = users.user_id
    where users.user_id is null
)

select * from final
