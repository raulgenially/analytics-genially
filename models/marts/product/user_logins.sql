with users as (
    select * from {{ ref('stg_users') }}
),

user_history as (
    select
        user_id,
        last_access_at,

    from {{ ref('stg_clean_snapshot_users') }}
    -- We started tracking changes on this date
    where last_access_at >= '{{ var('snapshot_users_start_date') }}'
),

-- We want to consider the registration date as a login as well.
logins_users_unioned as (
    select
        user_id,
        last_access_at as login_at

    from user_history

    union distinct

    select
        user_id,
        registered_at as login_at

    from users
),

-- In case we have several records for the same day, pick the last one.
logins_deduped as (
    {{
        unique_records_by_column(
            cte='logins_users_unioned',
            unique_column='user_id, date(login_at)',
            order_by='login_at',
            dir='desc',
        )
    }}
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_id',
            'date(login_at)'
           ])
        }} as login_id,

        user_id,

        login_at

    from logins_deduped
)

select * from final
