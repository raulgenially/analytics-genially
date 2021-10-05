-- Broad sector and broad role fields should not refer to old onboarding
with users as (
    select * from {{ ref('users') }}
),

final as (
    select
        user_id,
        broad_sector, 
        broad_role

    from users
    where broad_sector like '%(old)%'
        or broad_role like '%(old)%'
)

select * from final
