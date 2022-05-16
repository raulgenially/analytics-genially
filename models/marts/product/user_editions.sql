with snowplow_web_page_views as (
    select * from {{ ref('snowplow_web_page_views') }}
),

user_editors as (
    select distinct
        user_id,
        date(derived_tstamp) as edition_at

    from snowplow_web_page_views
    where date(start_tstamp) >= '{{ var('snowplow_page_views_start_date') }}' -- Table partitioned by start_tstamp
        and page_urlpath like '/editor%'
),

first_edition as (
    select
        user_id,
        min(edition_at) as first_touch_at

    from user_editors
    group by 1
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_editors.user_id',
            'user_editors.edition_at'
           ])
        }} as edition_id,

        user_editors.user_id,

        user_editors.edition_at,
        first_edition.first_touch_at as first_touch_at

    from user_editors
    left join first_edition
        on user_editors.user_id = first_edition.user_id
)

select * from final
