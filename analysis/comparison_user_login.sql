{% set date %}
    date('2022-02-08')
{% endset %}

with user_logins as (
    select * from {{ ref('user_logins') }}
),

ga4_events as (
    select * from {{ ref('src_ga4_events') }}
),

total_user_login as (
    select "1" as comparison, count(distinct user_id) as total_user_login
    from  user_logins
    WHERE date(login_at) = {{ date }}
),

total_ga4 as (
    select "1" as comparison, count(distinct user_id) as total_ga4
    from ga4_events
    where date(event_at) = {{ date }}
    and event_name='page_view' 
    and ( hostname = 'app.genial.ly'
        OR  
        (hostname = 'auth.genial.ly' 
            and page_location='https://auth.genial.ly/es/onboarding'
            and page_referrer='https://app.genial.ly/'))
)

select user.comparison, total_user_login, total_ga4
from total_user_login user
left join total_ga4 ga4
on user.comparison=ga4.comparison