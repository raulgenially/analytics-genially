with snowplow_web_page_views as (
    select * from {{ ref('snowplow_web_page_views') }}
    where user_id is not null --TODO to be changed after implementing user entity.
),

editor_visits as (
    select
        user_id,
        derived_tstamp as editor_visited_at

    from snowplow_web_page_views
    where date(start_tstamp) >= '{{ var('snowplow_page_views_start_date') }}' -- Table partitioned by start_tstamp.
        and page_urlpath like '/editor/%'
),

-- Pick the last edition for a certain day.
editor_visits_deduped as (
    {{
        unique_records_by_column(
            cte='editor_visits',
            unique_column='user_id, date(editor_visited_at)',
            order_by='editor_visited_at',
            dir='desc',
        )
    }}
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_id',
            'date(editor_visited_at)'
           ])
        }} as editor_visit_id,

        user_id,

        editor_visited_at,

    from editor_visits_deduped
)

select * from final
