with user_history as (
    select
        user_id,
        last_access_at,
        version,

    from {{ ref('src_snapshot_genially_users') }}
    where last_access_at is not null
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
        *,
        row_number() over (
            partition by user_id
            order by last_access_at
        ) as login_day,

    from logins
),

first_login as (
    select
        user_id,
        last_access_at as first_login_at

    from logins_numbered
    where login_day = 1
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'logins.user_id',
            'logins.last_access_at'
           ])
        }} as id,

        logins.user_id,

        date(logins.last_access_at) as login_at,
        date(first_login.first_login_at) as first_login_at

    from logins
    left join first_login
        on logins.user_id = first_login.user_id
)

select * from final
