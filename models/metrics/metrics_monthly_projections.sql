{% set week_days = 7 %}
{% set month_days = 28 %}
{% set year_days = 364 %}


{% set min_date %}
    date('2020-01-01')
{% endset %}

with reference_table as (
    {{ get_combination_calendar_dimensions(min_date) }}
),

geniallys as (
    select * from {{ ref('geniallys') }}
),

users as (
    select * from {{ ref('users') }}
),

country_codes as (
    select * from {{ ref('seed_country_codes') }}
),

collaboratives as (
    select * from {{ ref('collaboratives') }}
),

signups as (
    select 
        -- Dimensions
        date(users.registered_at) as registered_at,
        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.country,
        users.country_name,
        -- Metrics
        count(user_id) as n_signups
    from users
    where date(users.registered_at) >= {{ min_date }} -- Only focus on signups from min_date on
    {{ dbt_utils.group_by(n=5) }}
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
        --Metrics
        coalesce(signups.n_signups,0) as n_signups

    from reference_table
    left join signups
        on reference_table.date_day = signups.registered_at
            and reference_table.plan = signups.plan
            and reference_table.subscription = signups.subscription
            and reference_table.country = signups.country
            and reference_table.country_name = signups.country_name
),

creations as(
    select 
        -- Dimensions
        date(created_at) as created_at,
        geniallys.user_plan,
        {{ create_subscription_field('geniallys.user_plan') }} as user_subscription,
        ifnull(geniallys.user_country, '{{ var('not_selected') }}') as user_country,
        ifnull(country_codes.name, '{{ var('not_selected') }}') as user_country_name,
        -- Metrics
        count(distinct geniallys.genially_id) as n_creations
    
    from geniallys
    left join country_codes
            on geniallys.user_country = country_codes.code
    where date(created_at) >= {{ min_date }}    
    {{ dbt_utils.group_by(n=5) }}
),

metrics2 as (
    select 
        --Dimensions
        metrics1.date_day,
        metrics1.plan,
        metrics1.subscription,
        metrics1.country,
        metrics1.country_name,
        --Metrics
        metrics1.n_signups,
        coalesce(creations.n_creations,0) as n_creations

    from metrics1
    left join creations
        on metrics1.date_day = creations.created_at
            and metrics1.plan = creations.user_plan
            and metrics1.subscription = creations.user_subscription
            and metrics1.country = creations.user_country
            and metrics1.country_name = creations.user_country_name
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
--if a collaboration started before the user is registered, the creation date should be equal to user's registered date:
totalcreators as (
    select 
        uniquecreators.user_id,
        min (if (
                date(first_creation_at) < date(users.registered_at),
                date(users.registered_at),
                date(first_creation_at)
            )
        ) as first_creation_at
    from uniquecreators
    left join users
        on uniquecreators.user_id = users.user_id
    group by 1
),

new_creators as (
    select 
        --Dimensions
        date(first_creation_at) as first_creation_at,    
        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.country,
        users.country_name,
        -- Metrics
        count(distinct users.user_id) as n_new_creators

    from totalcreators
    inner join users 
        on totalcreators.user_id = users.user_id 
    where date(totalcreators.first_creation_at) >= {{ min_date }} 
        and users.registered_at is not null
    {{ dbt_utils.group_by(n=5) }}
),

metrics3 as (
    select 
        --Dimensions
        metrics2.date_day,
        metrics2.plan,
        metrics2.subscription,
        metrics2.country,
        metrics2.country_name,
        --Metrics
        metrics2.n_signups,
        metrics2.n_creations,
        coalesce(new_creators.n_new_creators,0) as n_new_creators

    from metrics2
    left join new_creators
        on metrics2.date_day = new_creators.first_creation_at
        and metrics2.plan = new_creators.plan
        and metrics2.subscription = new_creators.subscription
        and metrics2.country = new_creators.country
        and metrics2.country_name = new_creators.country_name
),

new_creators_registered_same_day as (
    select
        --Dimensions 
        date(first_creation_at) as first_creation_at,    
        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,        
        users.country,
        users.country_name,
        -- Metrics
        count(distinct users.user_id) as n_new_creators_registered_same_day

    from totalcreators
    inner join users 
        on totalcreators.user_id = users.user_id
        and date(totalcreators.first_creation_at) = date(users.registered_at) 
    where date(totalcreators.first_creation_at) >= {{ min_date }}     
        and users.registered_at is not null
    {{ dbt_utils.group_by(n=5) }}
),

metrics4 as (
    select 
        --Dimensions
        metrics3.date_day,
        metrics3.plan,
        metrics3.subscription,
        metrics3.country,
        metrics3.country_name,
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
        
    from metrics4
)

select * from final
