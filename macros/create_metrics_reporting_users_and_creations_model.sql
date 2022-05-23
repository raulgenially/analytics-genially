-- This macro is intended to be used in reporting-related models, across various granularities.
-- The granularity is determined by the input parameter 'date_part'.
{% macro create_metrics_reporting_users_and_creations_model(min_date, date_part) %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date, date_part) }}
),

users as (
    select
        *,
        date(date_trunc(registered_at, {{ date_part }})) as date_part_registered_at

    from {{ ref('all_users') }}
    where date(registered_at) >= {{ min_date }}
),

geniallys as (
    select
        *,
        date(date_trunc(created_at, {{ date_part }})) as date_part_created_at

    from {{ ref('all_geniallys') }}
    where date(created_at) >= {{ min_date }}
),

user_creations as (
    select
        *,
        date(date_trunc(creation_at, {{ date_part }})) as date_part_creation_at

    from {{ ref('user_creations') }}
    where date(creation_at) >= {{ min_date }}
),

user_logins as (
    select
        *,
        date(date_trunc(login_at, {{ date_part }})) as date_part_login_at

    from {{ ref('user_logins') }}
    where date(login_at) >= {{ min_date }}
),

-- New users / Signups
signups as (
    select
        -- Dimensions
        date_part_registered_at as registered_at,
        plan,
        subscription,
        country,
        country_name,
        broad_sector,
        broad_role,
        -- Metrics
        count(user_id) as n_signups

    from users
    {{ dbt_utils.group_by(n=7) }}
),

metrics1 as (
    select
        --Dimensions
        date(reference_table.date_{{ date_part }}) as date_part,
        reference_table.plan,
        reference_table.subscription,
        reference_table.country,
        reference_table.country_name,
        reference_table.broad_sector,
        reference_table.broad_role,
        --Metrics
        coalesce(signups.n_signups, 0) as n_signups

    from reference_table
    left join signups
        on date(reference_table.date_{{ date_part }}) = signups.registered_at
            and reference_table.plan = signups.plan
            and reference_table.subscription = signups.subscription
            and reference_table.country = signups.country
            and reference_table.country_name = signups.country_name
            and reference_table.broad_sector = signups.broad_sector
            and reference_table.broad_role = signups.broad_role
),

-- New creations
new_creations as(
    select
        -- Dimensions
        date_part_created_at as created_at,
        user_plan,
        {{ create_subscription_field('user_plan') }} as user_subscription,
        user_country,
        user_country_name,
        user_broad_sector,
        user_broad_role,
        -- Metrics
        count(genially_id) as n_new_creations

    from geniallys
    {{ dbt_utils.group_by(n=7) }}
),

metrics2 as (
    select
        --Dimensions
        metrics1.date_part,
        metrics1.plan,
        metrics1.subscription,
        metrics1.country,
        metrics1.country_name,
        metrics1.broad_sector,
        metrics1.broad_role,
        --Metrics
        metrics1.n_signups,
        coalesce(new_creations.n_new_creations, 0) as n_new_creations

    from metrics1
    left join new_creations
        on metrics1.date_part = new_creations.created_at
            and metrics1.plan = new_creations.user_plan
            and metrics1.subscription = new_creations.user_subscription
            and metrics1.country = new_creations.user_country
            and metrics1.country_name = new_creations.user_country_name
            and metrics1.broad_sector = new_creations.user_broad_sector
            and metrics1.broad_role = new_creations.user_broad_role
),

-- First creation for each user
user_first_creations as (
    select
        user_id,
        min(date_part_creation_at) as date_part_first_creation_at

    from user_creations
    group by 1
),

-- New creators
new_creators as (
    select
        --Dimensions
        user_first_creations.date_part_first_creation_at as first_creation_at,
        users.plan,
        users.subscription,
        users.country,
        users.country_name,
        users.broad_sector,
        users.broad_role,
        -- Metrics
        count(user_first_creations.user_id) as n_new_creators

    from user_first_creations
    inner join users
        on user_first_creations.user_id = users.user_id
    {{ dbt_utils.group_by(n=7) }}
),

metrics3 as (
    select
        --Dimensions
        metrics2.date_part,
        metrics2.plan,
        metrics2.subscription,
        metrics2.country,
        metrics2.country_name,
        metrics2.broad_sector,
        metrics2.broad_role,
        --Metrics
        metrics2.n_signups,
        metrics2.n_new_creations,
        coalesce(new_creators.n_new_creators, 0) as n_new_creators

    from metrics2
    left join new_creators
        on metrics2.date_part = new_creators.first_creation_at
            and metrics2.plan = new_creators.plan
            and metrics2.subscription = new_creators.subscription
            and metrics2.country = new_creators.country
            and metrics2.country_name = new_creators.country_name
            and metrics2.broad_sector = new_creators.broad_sector
            and metrics2.broad_role = new_creators.broad_role
),

-- New creators AND New users
new_creators_registered_same_period as (
    select
        --Dimensions
        user_first_creations.date_part_first_creation_at as first_creation_at,
        users.plan,
        users.subscription,
        users.country,
        users.country_name,
        users.broad_sector,
        users.broad_role,
        -- Metrics
        count(user_first_creations.user_id) as n_new_creators_registered_same_period

    from user_first_creations
    inner join users
        on user_first_creations.user_id = users.user_id
            and user_first_creations.date_part_first_creation_at = users.date_part_registered_at
    {{ dbt_utils.group_by(n=7) }}
),

metrics4 as (
    select
        --Dimensions
        metrics3.date_part,
        metrics3.plan,
        metrics3.subscription,
        metrics3.country,
        metrics3.country_name,
        metrics3.broad_sector,
        metrics3.broad_role,
        --Metrics
        metrics3.n_signups,
        metrics3.n_new_creations,
        metrics3.n_new_creators,
        coalesce(new_creators_registered_same_period.n_new_creators_registered_same_period, 0)
            as n_new_creators_registered_same_period,
        metrics3.n_new_creators - coalesce(new_creators_registered_same_period.n_new_creators_registered_same_period, 0)
            as n_new_creators_previously_registered

    from metrics3
    left join new_creators_registered_same_period
        on metrics3.date_part = new_creators_registered_same_period.first_creation_at
            and metrics3.plan = new_creators_registered_same_period.plan
            and metrics3.subscription = new_creators_registered_same_period.subscription
            and metrics3.country = new_creators_registered_same_period.country
            and metrics3.country_name = new_creators_registered_same_period.country_name
            and metrics3.broad_sector = new_creators_registered_same_period.broad_sector
            and metrics3.broad_role = new_creators_registered_same_period.broad_role
),

active_users as (
    select
        -- Dimensions
        user_logins.date_part_login_at as login_at,
        users.plan,
        users.subscription,
        users.country,
        users.country_name,
        users.broad_sector,
        users.broad_role,
        -- Metrics
        count(distinct user_logins.user_id) as n_active_users

    from user_logins
    inner join users
        on user_logins.user_id = users.user_id
    {{ dbt_utils.group_by(n=7) }}
),

final as (
    select
        --Dimensions
        metrics4.date_part as date_{{ date_part }},
        metrics4.plan,
        metrics4.subscription,
        metrics4.country,
        metrics4.country_name,
        metrics4.broad_sector,
        metrics4.broad_role,
        --Metrics
        metrics4.n_signups,
        metrics4.n_new_creations,
        metrics4.n_new_creators,
        metrics4.n_new_creators_registered_same_period as n_new_creators_registered_same_{{ date_part }},
        metrics4.n_new_creators_previously_registered,
        -- Variable 'snapshot_users_start_date' rounded so we can use this model for different granularities
        if(metrics4.date_part < '2022-01-01', null, coalesce(active_users.n_active_users, 0)) as n_active_users,
        if(
            metrics4.date_part < '2022-01-01', null, coalesce(active_users.n_active_users, 0)
        ) - n_signups as n_returning_users

    from metrics4
    left join active_users
        on metrics4.date_part = active_users.login_at
            and metrics4.plan = active_users.plan
            and metrics4.subscription = active_users.subscription
            and metrics4.country = active_users.country
            and metrics4.country_name = active_users.country_name
            and metrics4.broad_sector = active_users.broad_sector
            and metrics4.broad_role = active_users.broad_role
)

select * from final

{% endmacro %}
