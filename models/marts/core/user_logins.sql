with user_history as (
    select
        user_id,
        last_access_at,
        version,

    from {{ ref('stg_clean_snapshot_users') }}
    -- We started tracking changes on this date
    where last_access_at >= '{{ var('snapshot_users_start_date') }}'
),

-- In case we have several records for
-- the same day, pick the last one.
logins as (
    {{ unique_records_by_column(
        cte='user_history',
        unique_column='user_id, date(last_access_at)',
        order_by='version',
        dir='desc'
       )
    }}
),

logins_numbered as (
    select
        user_id,
        logins.last_access_at as login_at,
        row_number() over (
            partition by user_id
            order by last_access_at
        ) as login_day,

    from logins
),

first_touch as (
    select
        user_id,
        login_at as first_touch_at

    from logins_numbered
    where login_day = 1
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'logins.user_id',
            'logins.login_at'
           ])
        }} as login_id,

        logins.user_id,

        logins.login_at,
        first_touch.first_touch_at as first_touch_at

    from logins_numbered as logins
    left join first_touch
        on logins.user_id = first_touch.user_id
)

select * from final
