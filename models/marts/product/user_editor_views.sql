with snowplow_web_page_views as (
    select * from {{ ref('snowplow_web_page_views') }}
    where user_id is not null
),

editor_views as (
    select
        user_id,
        derived_tstamp as edition_at

    from snowplow_web_page_views
    where date(start_tstamp) >= '{{ var('snowplow_page_views_start_date') }}' -- Table partitioned by start_tstamp
        and page_urlpath like '/editor%'
),

-- Pick the last edition for a certain day
editor_views_deduped as (
    {{
        unique_records_by_column(
            cte='editor_views',
            unique_column='user_id, date(edition_at)',
            order_by='edition_at',
            dir='desc',
        )
    }}
),

final as (
    select
        {{ dbt_utils.surrogate_key([
            'user_id',
            'date(edition_at)'
           ])
        }} as edition_id,

        user_id,

        edition_at,

    from editor_views_deduped
)

select * from final
