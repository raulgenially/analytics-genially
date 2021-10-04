with user_data as (
    select * from {{ ref('adhoc_base_user_data_enrichment') }}
),


final as (
    select
        *

    from user_data
    -- The text after @ will be replaced by input from the user
    where email like '%@parisdescartes.fr'
)

select * from final

