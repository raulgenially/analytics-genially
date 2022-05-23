with geniallys as (
    select * from {{ ref('stg_geniallys') }}
    where created_at is not null
),

all_users as (
    select * from {{ ref('int_mart_all_users') }}
),

collaboratives as (
    select * from {{ ref('stg_collaboratives') }}
    where user_id is not null
),

cleaned_geniallys as ( -- Remove geniallys due to phishing users.
    select
        geniallys.*

    from geniallys
    inner join all_users
        on geniallys.user_id = all_users.user_id
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

    from collaboratives
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
