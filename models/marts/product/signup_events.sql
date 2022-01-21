{{
    config(
        materialized='incremental',
        cluster_by='user_id'
    )
}}

{% set tracking_date = '2021-11-01'%}
{% set two_days_ago = modules.datetime.date.today() - modules.datetime.timedelta(days=2) %}
{% set start_date = two_days_ago.strftime('%Y-%m-%d') if is_incremental() else tracking_date %}

with raw_events as (
    {{
        ga4_events_source(
            start_date=start_date,
            event='sign_up'
        )
    }}
),

valid_events as (
    select * from raw_events
    where user_id is not null
    {% if is_incremental() %}
        and user_id not in (select user_id from {{ this }})
    {% endif %}

),

unique_events as (
    {{
        unique_records_by_column(
            cte='valid_events',
            unique_column='user_id',
            order_by='event_at',
            dir='asc',
        )
    }}
),

final as (
    select
        user_id,

        device_category as device,
        ifnull(traffic_source, '{{ var('unknown') }}') as source,
        {{
            map_default_ga4_channel_grouping(
                'traffic_source',
                'traffic_source_medium',
                'traffic_source_name'
            )
        }} as channel,

        event_at

    from unique_events
)

select * from final
