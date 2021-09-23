-- Check that canceled licenses have a finished_at date.
-- We have some licenses where we don't have that data
-- Let's make sure that does not happen again

with licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

final as (
    select
        license_id,
        finished_at,
        status,
        started_at,

    from licenses
    where finished_at is null and status = 'Canceled'
        and started_at > timestamp('2020-02-05')
    order by started_at desc
)

select * from final
