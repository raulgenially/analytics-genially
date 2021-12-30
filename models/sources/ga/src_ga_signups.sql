with signups as (
    select * from {{ source('miscellaneous', 'ga4_signups') }}
),

final as (
    select
        user_id,

        category as device,
        replace(medium, '(none)',  'direct') as acquisition_channel,

        date as registered_at

    from signups
)

select * from final
