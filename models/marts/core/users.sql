with users as (
    select * from {{ ref('src_genially_users') }}
),

geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

-- TODO to be reviewed
previously_created_geniallys as (
    select
        genially_id,
        created_at,
        lag(created_at) 
            over (partition by user_id order by created_at asc) as previous_created_at,
        row_number()
            over (partition by user_id order by created_at asc) as rank,
        user_id,
        user_registered_at

    from geniallys
    where is_current_user = True
    order by user_id, created_at asc
),

users_adoption_velocity as (
    select
        user_id,
        _1 as days_btw_registration_and_first_creation,
        _2 as days_btw_first_and_second_creation,
        _3 as days_btw_second_and_third_creation,
        _1 + _2 + _3 as days_btw_registration_and_third_creation 
    
    from (
        select
            user_id,
            rank, 
            date_diff(created_at, ifnull(previous_created_at, user_registered_at), day) as diffdays 
   
        from previously_created_geniallys 
    )
    pivot (
        max(diffdays) for rank in (1, 2, 3)
    )
    order by user_id
),

users_creations as (
    select
        user_id,
        count(user_id) as n_total_creations,
        countif(is_deleted = False) as n_active_creations,
        countif(is_deleted = False and is_published = True) as n_published_creations,
        min(created_at) as first_creation_at,
        max(created_at) as last_creation_at

    from geniallys
    where is_current_user = True
    group by 1
),

final as (
    select
        users.user_id,

        users.subscription_plan as plan,
        users.sector,
        users.role,
        users.country as market,
        coalesce(users_creations.n_total_creations, 0) as n_total_creations,
        coalesce(users_creations.n_active_creations, 0) as n_active_creations,
        coalesce(users_creations.n_published_creations, 0) as n_published_creations,
        users_adoption_velocity.days_btw_registration_and_first_creation,
        users_adoption_velocity.days_btw_first_and_second_creation,
        users_adoption_velocity.days_btw_second_and_third_creation,
        users_adoption_velocity.days_btw_registration_and_third_creation,

        users.is_validated,

        users_creations.first_creation_at,
        users_creations.last_creation_at,
        users.registered_at,
        users.last_access_at

    from users
    left join users_creations
        on users.user_id = users_creations.user_id
    left join users_adoption_velocity
        on users.user_id = users_adoption_velocity.user_id
    where date(registered_at) >= date(2014, 1, 1)
)

select * from final