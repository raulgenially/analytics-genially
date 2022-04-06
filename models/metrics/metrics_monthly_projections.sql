{% set week_days = 7 %}
{% set month_days = 28 %}
{% set year_days = 364 %}

{% set min_date %}
    date('2020-01-01')
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date,"day") }}
),

geniallys as (
    select * from {{ ref('geniallys') }}
),

users as (
    select * from {{ ref('users') }}
),

collaboratives as (
    select * from {{ ref('collaboratives') }}
),

user_logins as (
    select * from {{ ref('user_logins') }}
),

-- We want to consider the registration date as a login as well.
logins as (
    select
        user_id,
        date(login_at) as login_at

    from user_logins

    union distinct

    select
        user_id,
        date(registered_at) as login_at

    from users
),

signups as (
    select
        -- Dimensions
        date(registered_at) as registered_at,
        plan,
        subscription,
        country,
        country_name,
        broad_sector,
        broad_role,
        -- Metrics
        count(user_id) as n_signups

    from users
    where date(registered_at) >= {{ min_date }} -- Only focus on signups from min_date on
    {{ dbt_utils.group_by(n=7) }}
),

--reference_signups
metrics1 as (
    select
        --Dimensions
        reference_table.date_day,
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
        on reference_table.date_day = signups.registered_at
            and reference_table.plan = signups.plan
            and reference_table.subscription = signups.subscription
            and reference_table.country = signups.country
            and reference_table.country_name = signups.country_name
            and reference_table.broad_sector = signups.broad_sector
            and reference_table.broad_role = signups.broad_role
),

creations as(
    select
        -- Dimensions
        date(created_at) as created_at,
        user_plan,
        {{ create_subscription_field('user_plan') }} as user_subscription,
        user_country,
        user_country_name,
        user_broad_sector,
        user_broad_role,
        -- Metrics
        count(genially_id) as n_creations

    from geniallys
    where date(created_at) >= {{ min_date }}
    {{ dbt_utils.group_by(n=7) }}
),

metrics2 as (
    select
        --Dimensions
        metrics1.date_day,
        metrics1.plan,
        metrics1.subscription,
        metrics1.country,
        metrics1.country_name,
        metrics1.broad_sector,
        metrics1.broad_role,
        --Metrics
        metrics1.n_signups,
        coalesce(creations.n_creations, 0) as n_creations

    from metrics1
    left join creations
        on metrics1.date_day = creations.created_at
            and metrics1.plan = creations.user_plan
            and metrics1.subscription = creations.user_subscription
            and metrics1.country = creations.user_country
            and metrics1.country_name = creations.user_country_name
            and metrics1.broad_sector = creations.user_broad_sector
            and metrics1.broad_role = creations.user_broad_role
),

creators_usersfromgeniallys as (
    select
        user_id,
        min(created_at) as first_creation_at

    from geniallys
    group by 1
),

creators_usersfromcollaboratives as (
    select
        user_id,
        min(created_at) as first_creation_at

    from collaboratives
    where user_id is not null
    group by 1
),

creators as (
    select
        user_id,
        first_creation_at

    from creators_usersfromgeniallys

    union all

    select
        user_id,
        first_creation_at

    from creators_usersfromcollaboratives
),

uniquecreators as(
    select
        user_id,
        min(first_creation_at) as first_creation_at,

    from creators
    group by 1
),
--if a collaboration or a genially started before the user is registered, the creation date should be equal to user's registered date:

totalcreators as (
    select
        uniquecreators.user_id,
        if (
            date(uniquecreators.first_creation_at) < date(users.registered_at),
            date(users.registered_at),
            date(uniquecreators.first_creation_at)
        ) as first_creation_at

    from uniquecreators
    left join users
        on uniquecreators.user_id = users.user_id
),

new_creators as (
    select
        --Dimensions
        date(totalcreators.first_creation_at) as first_creation_at,
        users.plan,
        users.subscription,
        users.country,
        users.country_name,
        users.broad_sector,
        users.broad_role,
        -- Metrics
        count(distinct users.user_id) as n_new_creators

    from totalcreators
    left join users
        on totalcreators.user_id = users.user_id
    where date(totalcreators.first_creation_at) >= {{ min_date }}
        and users.registered_at is not null
    {{ dbt_utils.group_by(n=7) }}
),

metrics3 as (
    select
        --Dimensions
        metrics2.date_day,
        metrics2.plan,
        metrics2.subscription,
        metrics2.country,
        metrics2.country_name,
        metrics2.broad_sector,
        metrics2.broad_role,
        --Metrics
        metrics2.n_signups,
        metrics2.n_creations,
        coalesce(new_creators.n_new_creators, 0) as n_new_creators

    from metrics2
    left join new_creators
        on metrics2.date_day = new_creators.first_creation_at
        and metrics2.plan = new_creators.plan
        and metrics2.subscription = new_creators.subscription
        and metrics2.country = new_creators.country
        and metrics2.country_name = new_creators.country_name
        and metrics2.broad_sector = new_creators.broad_sector
        and metrics2.broad_role = new_creators.broad_role
),

new_creators_registered_same_day as (
    select
        --Dimensions
        date(first_creation_at) as first_creation_at,
        users.plan,
        users.subscription,
        users.country,
        users.country_name,
        users.broad_sector,
        users.broad_role,
        -- Metrics
        count(users.user_id) as n_new_creators_registered_same_day

    from totalcreators
    left join users
        on totalcreators.user_id = users.user_id
        and date(totalcreators.first_creation_at) = date(users.registered_at)
    where date(totalcreators.first_creation_at) >= {{ min_date }}
    {{ dbt_utils.group_by(n=7) }}
),

metrics4 as (
    select
        --Dimensions
        metrics3.date_day,
        metrics3.plan,
        metrics3.subscription,
        metrics3.country,
        metrics3.country_name,
        metrics3.broad_sector,
        metrics3.broad_role,
        --Metrics
        metrics3.n_signups,
        metrics3.n_creations,
        metrics3.n_new_creators,
        coalesce(new_creators_registered_same_day.n_new_creators_registered_same_day, 0) as n_new_creators_registered_same_day,
        metrics3.n_new_creators - coalesce(new_creators_registered_same_day.n_new_creators_registered_same_day, 0) as n_new_creators_previously_registered

    from metrics3
    left join new_creators_registered_same_day
        on metrics3.date_day = new_creators_registered_same_day.first_creation_at
        and metrics3.plan = new_creators_registered_same_day.plan
        and metrics3.subscription = new_creators_registered_same_day.subscription
        and metrics3.country = new_creators_registered_same_day.country
        and metrics3.country_name = new_creators_registered_same_day.country_name
        and metrics3.broad_sector = new_creators_registered_same_day.broad_sector
        and metrics3.broad_role = new_creators_registered_same_day.broad_role
),

user_logins_profile as (
    select
        logins.user_id,
        logins.login_at,

        users.plan,
        users.subscription,
        users.country,
        users.country_name,
        users.broad_sector,
        users.broad_role

    from logins
    left join users
        on logins.user_id = users.user_id
    where logins.login_at >= {{ min_date }}
),

total_visitors as (
    select
        --Dimensions
        login_at,
        plan,
        subscription,
        country,
        country_name,
        broad_sector,
        broad_role,
        --Metrics
        count(user_id) as n_total_visitors

    from user_logins_profile
    {{ dbt_utils.group_by(n=7) }}
),

metrics5 as (
    select
        --Dimensions
        metrics4.date_day,
        metrics4.plan,
        metrics4.subscription,
        metrics4.country,
        metrics4.country_name,
        metrics4.broad_sector,
        metrics4.broad_role,
        --Metrics
        metrics4.n_signups,
        metrics4.n_creations,
        metrics4.n_new_creators,
        metrics4.n_new_creators_registered_same_day,
        metrics4.n_new_creators_previously_registered,
        case
            when metrics4.date_day < '{{ var('snapshot_users_start_date') }}'
                then null
            else coalesce(total_visitors.n_total_visitors, 0)
        end as n_total_visitors,

    from metrics4
    left join total_visitors
        on metrics4.date_day = total_visitors.login_at
        and metrics4.plan = total_visitors.plan
        and metrics4.subscription = total_visitors.subscription
        and metrics4.country = total_visitors.country
        and metrics4.country_name = total_visitors.country_name
        and metrics4.broad_sector = total_visitors.broad_sector
        and metrics4.broad_role = total_visitors.broad_role

),

final as (
    select
        *,
        -- lags n_signups
        {{ get_lag_dimension_monthly_projections('n_signups', week_days) }} as n_signups_previous_7d,
        {{ get_lag_dimension_monthly_projections('n_signups', month_days) }} as n_signups_previous_28d,
        {{ get_lag_dimension_monthly_projections('n_signups', year_days) }} as n_signups_previous_364d,
        -- lags n_creations
        {{ get_lag_dimension_monthly_projections('n_creations', week_days) }} as n_creations_previous_7d,
        {{ get_lag_dimension_monthly_projections('n_creations', month_days) }} as n_creations_previous_28d,
        {{ get_lag_dimension_monthly_projections('n_creations', year_days) }} as n_creations_previous_364d,
        -- lag n_new_creators
        {{ get_lag_dimension_monthly_projections('n_new_creators', week_days) }} as n_new_creators_previous_7d,
        {{ get_lag_dimension_monthly_projections('n_new_creators', month_days) }} as n_new_creators_previous_28d,
        {{ get_lag_dimension_monthly_projections('n_new_creators', year_days) }} as n_new_creators_previous_364d,
        -- lag n_new_creators_registered_same_day
        {{ get_lag_dimension_monthly_projections('n_new_creators_registered_same_day', week_days) }} as n_new_creators_registered_same_day_previous_7d,
        {{ get_lag_dimension_monthly_projections('n_new_creators_registered_same_day', month_days) }} as n_new_creators_registered_same_day_previous_28d,
        {{ get_lag_dimension_monthly_projections('n_new_creators_registered_same_day', year_days) }} as n_new_creators_registered_same_day_previous_364d,
        -- lag n_new_creators_previously_registered
        {{ get_lag_dimension_monthly_projections('n_new_creators_previously_registered', week_days) }} as n_new_creators_previously_registered_previous_7d,
        {{ get_lag_dimension_monthly_projections('n_new_creators_previously_registered', month_days) }} as n_new_creators_previously_registered_previous_28d,
        {{ get_lag_dimension_monthly_projections('n_new_creators_previously_registered', year_days) }} as n_new_creators_previously_registered_previous_364d,
        --lag n_total_visitors
        {{ get_lag_dimension_monthly_projections('n_total_visitors', week_days) }} as n_total_visitors_previous_7d,
        {{ get_lag_dimension_monthly_projections('n_total_visitors', month_days) }} as n_total_visitors_previous_28d,
        {{ get_lag_dimension_monthly_projections('n_total_visitors', year_days) }} as n_total_visitors_previous_364d,

    from metrics5
)

select * from final
