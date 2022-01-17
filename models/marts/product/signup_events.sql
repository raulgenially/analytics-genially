{{
    config(
        materialized='incremental',
        cluster_by='user_id'
    )
}}

{% set tracking_date = '2021-02-23'%}
{% set yesterday = modules.datetime.date.today() - modules.datetime.timedelta(days=1) %}
{% set start_date = yesterday.strftime('%Y-%m-%d') if is_incremental() else tracking_date %}

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
        case
            when traffic_source_medium = '(none)'
                then 'direct'
            when traffic_source_medium is null
                then '{{ var('unknown') }}'
            else traffic_source_medium
        end as acquisition_channel,

        event_at

    from unique_events
)

select * from final
