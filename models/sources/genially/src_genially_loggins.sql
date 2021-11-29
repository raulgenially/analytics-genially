with loggins as (
    select * from {{ source('miscellaneous', 'logins') }}
),

final as (
    select
        date as login_at,
        user_id,
        event_name,
        hostname,
        first_touch_at,

    from loggins
)

select * from final
