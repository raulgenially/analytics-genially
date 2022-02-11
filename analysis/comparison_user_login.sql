--With this analysis, we want to compare differences in total logins metric using backend (last access time) or ga4 as the source.
--Currently, we have 2 comparisons:
-- -First comparison: Taking account in ga4 just page views of app.genial.ly and page views of auth.genial.ly of users who didn't finish the onboarding
-- -The second comparison: Taking account in ga4 all events except experiment_loaded

{% set date_to_analyse %}
    date('2022-02-08')
{% endset %}

with user_logins as (
    select * from {{ ref('user_logins') }}
),

ga4_events as (
    select * from {{ ref('src_ga4_events') }}
),

total_user_login as (
    select
        '1' as comparison,
        count(distinct user_id) as total_user_login

    from user_logins
    where date(login_at) = {{ date_to_analyse }}
),

total_ga4 as (
    select
        '1' as comparison,
        count(distinct user_id) as total_ga4

    from ga4_events
    where table_suffix = format_date('%Y%m%d', {{ date_to_analyse }})
        and event_name = 'page_view'
        and (hostname = 'app.genial.ly'
            or
            (hostname = 'auth.genial.ly'
                and page_location = 'https://auth.genial.ly/es/onboarding'
                and page_referrer = 'https://app.genial.ly/'))
),

first_comparison as (
    select
        user.comparison,
        total_user_login,
        total_ga4,
        (total_user_login - total_ga4) as difference,
        round(((total_user_login - total_ga4) / total_ga4) * 100 , 2) as variation

    from total_user_login user
    left join total_ga4 ga4
        on user.comparison = ga4.comparison
),

total_user_login_2 as (
    select
        '2' as comparison,
        count(distinct user_id) as total_user_login_2

    from user_logins
    where date(login_at) = {{ date_to_analyse }}
),

total_ga4_2 as (
    select
        '2' as comparison,
        count(distinct user_id) as total_ga4_2

    from ga4_events
    where table_suffix = format_date('%Y%m%d', {{ date_to_analyse }})
        and event_name <> 'experiment_loaded'
),

second_comparison as (
    select
        user.comparison,
        total_user_login_2,
        total_ga4_2,
        (total_user_login_2 - total_ga4_2) as difference,
        round(((total_user_login_2 - total_ga4_2) / total_ga4_2) * 100 , 2) as variation

    from total_user_login_2 user
    left join total_ga4_2 ga4
        on user.comparison = ga4.comparison
),

final as (
    select * from first_comparison
    union all
    select * from second_comparison
)

select * from final
