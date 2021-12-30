with user_data as (
    select * from {{ ref('adhoc_base_user_data_enrichment') }}
),


final as (
    select
        *

    from user_data
    -- These emails are used as an example. In the google sheet this is read from an input sheet
    where email in (
            "luiscebrian@genially.com",
            "antonio@genially.com",
            "raul@genial.ly"
        )
    order by email
)

select * from final
