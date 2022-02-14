with licenses as (
    select * from {{ ref('stg_licenses') }}
)

select * from licenses
