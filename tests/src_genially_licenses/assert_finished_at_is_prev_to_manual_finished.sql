-- Check that finished_at is previous to manual_finished_at
with licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

final as (
    select
        *

    from licenses
    where finished_at > manual_finished_at
)

select * from final
