{%- set min_date -%}
    date(2019, 1, 1)
{%- endset -%}

with users as (
    select * from {{ ref('users') }}
),

final as (
    select
        -- Dimensions
        date(registered_at) as registered_at,
        plan,
        subscription,
        sector,
        broad_sector,
        role,
        broad_role,
        country,
        country_name,

        -- Metrics
        count(user_id) as n_signups,
        countif(is_creator = true) as n_creators,
        countif(is_publisher = true) as n_publishers,
        countif(is_recurrent_publisher = true) as n_recurrent_publishers,
        countif(is_heavy_publisher = true) as n_heavy_publishers

    from users
    where date(registered_at) >= {{ min_date }}
        and date(registered_at) < current_date()
    {{ dbt_utils.group_by(n=9) }}
    order by registered_at asc
)

select * from final
