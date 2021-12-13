with logins as (
    select * from {{ source('miscellaneous', 'logins') }}
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_id',
            'date'
        ]) }} as login_id,

        event_name,
        hostname as host_name,

        user_id,

        date as login_at,
        first_touch_at

    from logins
)

select * from final
