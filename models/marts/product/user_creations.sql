with geniallys as (
    select * from {{ ref('stg_geniallys') }}
    where created_at is not null
),

users as (
    select * from {{ ref('int_mart_all_users') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
    where user_id is not null
),

-- Remove geniallys due to phishing users.
cleaned_geniallys as (
    select
        geniallys.*

    from geniallys
    inner join users
        on geniallys.user_id = users.user_id
),

-- Correct collaboratives shared before registration date.
cleaned_collaboratives as (
    select
        collaboratives.user_id,
        if(
            collaboratives.created_at < users.registered_at,
            users.registered_at,
            collaboratives.created_at
        ) as created_at

    from collaboratives
    inner join users
        on collaboratives.user_id = users.user_id
),

-- We want to consider collaboratives as well.
user_creations as (
    select
        user_id,
        created_at as creation_at

    from cleaned_geniallys

    union distinct

    select
        user_id,
        created_at as creation_at

    from cleaned_collaboratives
),

-- In case we have several records for the same day, pick the last one.
user_creations_deduped as (
    {{
        unique_records_by_column(
            cte='user_creations',
            unique_column='user_id, date(creation_at)',
            order_by='creation_at',
            dir='desc',
        )
    }}
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_id',
            'date(creation_at)'
           ])
        }} as creation_id,

        user_id,

        creation_at

    from user_creations_deduped
)

select * from final
