{% set week_days = 7 %}
{% set week_days_minus = week_days - 1 %}

{% set month_days = 28 %}
{% set month_days_minus = month_days - 1 %}

{% set min_date %}
    date('2021-11-01')
{% endset %}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=min_date,
        end_date="current_date()"
        )
    }}
),

geniallys as (
    select * from {{ ref('geniallys') }}
),

users as (
    select * from {{ ref('users') }}
),

country_codes as (
    select * from {{ ref('country_codes') }}
),

collaboratives as (
    select * from {{ ref('collaboratives') }}
),



signups as (
    select 
        -- Dimensions
        date(dates.date_day) as date_day,
        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.sector,
        users.broad_sector,
        users.role,
        users.broad_role,
        users.country,
        users.country_name,

        -- Metrics
        count(user_id) as n_signups
    from users
    cross join dates
    where date(users.registered_at) >= {{ min_date }} -- Only focus on signups from min_date on
        and dates.date_day = date(users.registered_at)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9    
    order by 1
),    

creations as(
select 
        -- Dimensions
        date(created_at) as created_at,
        geniallys.user_plan,
        {{ create_subscription_field('geniallys.user_plan') }} as user_subscription,
        geniallys.user_sector,
        geniallys.user_broad_sector,
        geniallys.user_role,
        geniallys.user_broad_role,
        geniallys.user_country,
        ifnull(country_codes.name, '{{ var('not_selected') }}') as user_country_name,
        -- Metrics
        count(distinct geniallys.genially_id) as n_creations
    
from geniallys
left join country_codes
        on geniallys.user_country = country_codes.code
cross join dates
where date(created_at)>={{ min_date }}
    and dates.date_day = date(geniallys.created_at)
group by 1,2,3,4,5,6,7,8,9
order by 1
),


metrics_1 as(
    select
        -- Dimensions
        if (date(signups.date_day) is null, creations.created_at, date(signups.date_day)) as date_day,
        if(signups.plan is null, creations.user_plan, signups.plan) as plan,
        if(signups.plan is null, {{ create_subscription_field('creations.user_plan') }},{{ create_subscription_field('signups.plan') }}) as subscription,
        if(signups.sector is null,creations.user_sector, signups.sector) as sector,
        if(signups.broad_sector is null, creations.user_broad_sector, signups.broad_sector) as broad_sector,
        if(signups.role is null, creations.user_role, signups.role) as role,
        if(signups.broad_role is null, creations.user_broad_role, signups.broad_role) as broad_role,
        if(signups.country is null,creations.user_country, signups.country) as country,
        if(signups.country_name is null, creations.user_country_name, signups.country_name) as country_name,         
        -- Metrics
        coalesce(cast(n_signups as int64),0) as n_signups,
        coalesce(cast(n_creations as int64),0) as n_creations
        
    from signups
    full outer join creations
        on signups.date_day = creations.created_at
            and signups.plan = creations.user_plan
            and signups.subscription = creations.user_subscription
            and signups.sector = creations.user_sector
            and signups.broad_sector = creations.user_broad_sector
            and signups.role = creations.user_role
            and signups.broad_role = creations.user_broad_role
            and signups.country = creations.user_country
            and signups.country_name = creations.user_country_name
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
    group by 1),

creators as (
    select
		user_id,
		first_creation_at
		
    from creators_usersfromgeniallys 
    
	union all 
    
	select 
		user_id,
		first_creation_at 
		
    from creators_usersfromcollaboratives),

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
        min (if (date(first_creation_at)< date(users.registered_at),date(users.registered_at),date(first_creation_at))) as first_creation_at
    from uniquecreators
    left join users
        on uniquecreators.user_id= users.user_id
    group by 1
),

new_creators as (
    select 
	    date(first_creation_at) as first_creation_at,    
        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.sector,
        users.broad_sector,
        users.role,
        users.broad_role,
        users.country,
        users.country_name,
    -- Metrics
	count(distinct users.user_id) as n_new_creators
	
from totalcreators
inner join users 
	on totalcreators.user_id = users.user_id 
cross join dates
where date(totalcreators.first_creation_at) >= {{ min_date }} 
    and dates.date_day = date(totalcreators.first_creation_at)
    and users.registered_at is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    order by 1
),

metrics_2 as(
    select
        -- Dimensions
        if (date(metrics_1.date_day) is null, new_creators.first_creation_at, date(metrics_1.date_day)) as date_day,
        if(metrics_1.plan is null, new_creators.plan, metrics_1.plan) as plan,
        if(metrics_1.plan is null, {{ create_subscription_field('new_creators.plan') }} , {{ create_subscription_field('metrics_1.plan') }}) as subscription,
        if(metrics_1.sector is null, new_creators.sector, metrics_1.sector) as sector,
        if(metrics_1.broad_sector is null, new_creators.broad_sector, metrics_1.broad_sector) as broad_sector,
        if(metrics_1.role is null, new_creators.role, metrics_1.role) as role,
        if(metrics_1.broad_role is null, new_creators.broad_role, metrics_1.broad_role) as broad_role,
        if(metrics_1.country is null, new_creators.country, metrics_1.country) as country,
        if(metrics_1.country_name is null, new_creators.country_name, metrics_1.country_name) as country_name,         
         -- Metrics
        coalesce(cast(metrics_1.n_signups as int64),0) as n_signups,
        coalesce(cast(metrics_1.n_creations as int64),0) as n_creations,
        coalesce(cast(new_creators.n_new_creators as int64),0) as n_new_creators  

    from metrics_1
    full outer join new_creators
        on metrics_1.date_day = new_creators.first_creation_at
            and metrics_1.plan = new_creators.plan
            and metrics_1.subscription = new_creators.subscription
            and metrics_1.sector = new_creators.sector
            and metrics_1.broad_sector = new_creators.broad_sector
            and metrics_1.role = new_creators.role
            and metrics_1.broad_role = new_creators.broad_role
            and metrics_1.country = new_creators.country
            and metrics_1.country_name = new_creators.country_name
    order by date_day
),

new_creators_registered_same_day as (
    select 
	    date(first_creation_at) as first_creation_at,    
        users.plan,
        {{ create_subscription_field('users.plan') }} as subscription,
        users.sector,
        users.broad_sector,
        users.role,
        users.broad_role,
        users.country,
        users.country_name,
    -- Metrics
	count(distinct users.user_id) as n_new_creators_registered_same_day
	
from totalcreators
inner join users 
	on totalcreators.user_id = users.user_id
    and date(totalcreators.first_creation_at)=date(users.registered_at) 
cross join dates
where date(totalcreators.first_creation_at) >= {{ min_date }} 
    and dates.date_day = date(totalcreators.first_creation_at)
    and users.registered_at is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    order by 1
),


final as(
    select
        -- Dimensions
        if (date(metrics_2.date_day) is null, new_creators_registered_same_day.first_creation_at, date(metrics_2.date_day)) as date_day,
        if(metrics_2.plan is null, new_creators_registered_same_day.plan, metrics_2.plan) as plan,
        if(metrics_2.plan is null, {{ create_subscription_field('new_creators_registered_same_day.plan') }} , {{ create_subscription_field('metrics_2.plan') }}) as subscription,
        if(metrics_2.sector is null, new_creators_registered_same_day.sector, metrics_2.sector) as sector,
        if(metrics_2.broad_sector is null, new_creators_registered_same_day.broad_sector, metrics_2.broad_sector) as broad_sector,
        if(metrics_2.role is null, new_creators_registered_same_day.role, metrics_2.role) as role,
        if(metrics_2.broad_role is null, new_creators_registered_same_day.broad_role, metrics_2.broad_role) as broad_role,
        if(metrics_2.country is null, new_creators_registered_same_day.country, metrics_2.country) as country,
        if(metrics_2.country_name is null, new_creators_registered_same_day.country_name, metrics_2.country_name) as country_name,         
         -- Metrics
        coalesce(cast(metrics_2.n_signups as int64),0) as n_signups,
        coalesce(cast(metrics_2.n_creations as int64),0) as n_creations,
        coalesce(cast(metrics_2.n_new_creators as int64),0) as n_new_creators,        
        coalesce(cast(new_creators_registered_same_day.n_new_creators_registered_same_day as int64),0) as n_new_creators_registered_same_day,
        coalesce(cast(metrics_2.n_new_creators as int64),0) - coalesce(cast(new_creators_registered_same_day.n_new_creators_registered_same_day as int64),0) as n_new_creators_previously_registered       

    from metrics_2
    full outer join new_creators_registered_same_day
        on metrics_2.date_day = new_creators_registered_same_day.first_creation_at
            and metrics_2.plan = new_creators_registered_same_day.plan
            and metrics_2.subscription = new_creators_registered_same_day.subscription
            and metrics_2.sector = new_creators_registered_same_day.sector
            and metrics_2.broad_sector = new_creators_registered_same_day.broad_sector
            and metrics_2.role = new_creators_registered_same_day.role
            and metrics_2.broad_role = new_creators_registered_same_day.broad_role
            and metrics_2.country = new_creators_registered_same_day.country
            and metrics_2.country_name = new_creators_registered_same_day.country_name
    order by    date_day,
                plan,
                subscription,
                sector,
                broad_sector,
                country,
                country_name
),

final2 as (
    select 
    date_day,
    plan,
    subscription,
    sector,
    broad_sector,
    role,
    broad_role,
    country,
    country_name,
    n_signups,
    n_creations,
    n_new_creators,
    n_new_creators_registered_same_day,
    n_new_creators_previously_registered,
    lag(n_signups, {{ week_days }}) over (
            partition by 
                plan,
                subscription,
                sector,
                broad_sector,
                role,
                broad_role,
                country,
                country_name
            order by 
                date_day asc
    ) as n_signups_previous_7d

    from final
)

select * from final2 where date_day >'2021-11-07'
--select * from final where date_day='2021-11-15'
--where n_signups_previous_7d <> 0 and n_signups_previous_7d is not null*/


/*select  date_day,
sum(n_signups) as n_signups,
sum(n_creations) as n_creations,
sum(n_new_creators) as n_new_creators,
sum(n_new_creators_registered_same_day) as n_new_creators_registered_same_day,
sum(n_new_creators_previously_registered) as n_new_creators_previously_registered,
sum(n_signups_previous_7d) as n_signups_previous_7d
from final2
group by 1
order by 1*/
