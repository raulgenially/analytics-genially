with snowplow_web_page_views as (
    select * from {{ ref('snowplow_web_page_views') }}
    where user_id is not null --TODO to be changed after implementing user entity.
),

editor_views as (
    select
        user_id,
        derived_tstamp as editor_view_at

    from snowplow_web_page_views
    where date(start_tstamp) >= '{{ var('snowplow_page_views_start_date') }}' -- Table partitioned by start_tstamp.
        and page_urlpath like '/editor/%'
),

-- Pick the last edition for a certain day.
editor_views_deduped as (
    {{
        unique_records_by_column(
            cte='editor_views',
            unique_column='user_id, date(editor_view_at)',
            order_by='editor_view_at',
            dir='desc',
        )
    }}
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_id',
            'date(editor_view_at)'
           ])
        }} as edition_id,

        user_id,

        editor_view_at,

    from editor_views_deduped
)

select * from final
