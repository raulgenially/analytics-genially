with raw_events as (
    select * from {{ ref('src_ga4_events') }}
    where event_name = 'sign_up'
        and user_id is not null
),

unique_events as (
    {{
        unique_records_by_column(
            cte='raw_events',
            unique_column='user_id',
            order_by='event_at',
            dir='asc',
        )
    }}
),

final as (
    select
        user_id,

        device.category as device,
        ifnull(traffic_source.source, '{{ var('unknown') }}') as source,
        case
            when traffic_source.medium = '(none)'
                then 'direct'
            when traffic_source.medium is null
                then '{{ var('unknown') }}'
            else traffic_source.medium
        end as acquisition_channel,

        event_at as registered_at,

    from unique_events
)

select * from final
