with events as (
    select * from {{ source('analytics_261787761', 'events') }}
),

numbered_events as (
    select
        *,
        row_number() over(
            partition by user_pseudo_id, event_timestamp, event_name
        ) as n_event
    from events
),

final as (
    select
        {{
            dbt_utils.surrogate_key([
                'event_name',
                'user_pseudo_id',
                'event_timestamp',
                'n_event'
            ])
        }} as id,

        event_name,
        event_params,
        event_value_in_usd,
        user_properties,
        user_ltv,
        device,
        geo,
        traffic_source,
        ecommerce,
        items,

        user_id,
        user_pseudo_id,

        timestamp_micros(event_timestamp) as event_at,
        timestamp_micros(user_first_touch_timestamp) as first_touch_at,

    from numbered_events
)

select * from final
