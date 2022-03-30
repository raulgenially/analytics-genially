-- This macro returns a reference table with all combinations of calendar dates (from min_date to current date) and the
-- dimensions: plans, subscriptions, countrys, country names, broad_sector and broad_role.
-- It is useful for metrics model to report all possible combinations not just the ones having place.
-- It should be the starting table, and then metrics should be added using left join to it.
{% macro get_combination_calendar_dimensions(min_date) %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=min_date,
        end_date="current_date()"
        )
    }}
),

plans as (
    select * from {{ ref('seed_plan') }}
),

country_codes as (
    select * from {{ ref('seed_country_codes') }}
    union all
    select
        '{{ var('not_selected') }}' as code,
        '{{ var('not_selected') }}' as name
),

broad_sector_role as (
    select
        distinct broad_sector, broad_role
    from {{ ref('users') }}
),

dates_plan as (
    select
        date(dates.date_day) as date_day,
        plans.plan,
        {{ create_subscription_field('plans.plan') }} as subscription

    from dates
    cross join plans
),

dates_plan_country as (
    select
        dates_plan.date_day,
        dates_plan.plan,
        dates_plan.subscription,
        country_codes.code as country,
        country_codes.name as country_name

    from dates_plan
    cross join country_codes
),

reference_table as (
    select
        dates_plan_country.date_day,
        dates_plan_country.plan,
        dates_plan_country.subscription,
        dates_plan_country.country,
        dates_plan_country.country_name,
        broad_sector_role.broad_sector,
        broad_sector_role.broad_role

    from dates_plan_country
    cross join broad_sector_role
)

select * from reference_table

{% endmacro %}
