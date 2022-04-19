{% set min_date %}
    date('2022-01-01') -- we are collecting data since '2021-12-20', so first complete month is 01-2022.
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date, "month") }}
),

returning_users as (
    select
        *,
        date_trunc(date_day, month) as date_month

    from {{ ref('metrics_login_activity') }}
    where status = 'Returning'
        and date_day >= {{ min_date }}
),

final as (
    select
        date(reference_table.date_month) as date_month,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        count(distinct returning_users.user_id) as n_returning

    from reference_table
    left join returning_users
        on date(reference_table.date_month) = returning_users.date_month
            and reference_table.plan = returning_users.plan
            and reference_table.subscription = returning_users.subscription
            and reference_table.country = returning_users.country
            and reference_table.country_name = returning_users.country_name
            and reference_table.broad_sector = returning_users.broad_sector
            and reference_table.broad_role = returning_users.broad_role
    {{ dbt_utils.group_by(n=7) }}
)

select * from final
