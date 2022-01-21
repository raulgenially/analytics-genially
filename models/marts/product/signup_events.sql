{{
    config(
        materialized='incremental'
    )
}}

{% set tracking_date = '2021-11-01'%}
{% set two_days_ago = modules.datetime.date.today() - modules.datetime.timedelta(days=2) %}
{% set start_date = two_days_ago.strftime('%Y-%m-%d') if is_incremental() else tracking_date %}

with raw_events as (
    select * from {{ ref('src_ga4_events') }}
    where event_name = 'sign_up'
        and user_id is not null
        and {{ sharded_date_range(start_date) }}
),

valid_events as (
    select * from raw_events
    {% if is_incremental() %}
        where user_id not in (select user_id from {{ this }})
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
