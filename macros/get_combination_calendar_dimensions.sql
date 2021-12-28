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

dates_plan as (
    select
        date(dates.date_day) as date_day,
        plans.plan,
        {{ create_subscription_field('plans.plan') }} as subscription

    from dates
    cross join plans
),

reference_table as (
    select
        dates_plan.date_day,
        dates_plan.plan,
        dates_plan.subscription,
        country_codes.code as country,
        country_codes.name as country_name

    from dates_plan
    cross join country_codes
)

select * from reference_table

{% endmacro %}
