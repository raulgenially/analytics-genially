-- The license codes are present in license_codes.csv
with license_codes as (
    select * from {{ ref('license_codes') }}
),

licenses as (
    select * from {{ ref('src_genially_licenses') }}
),

final as (
    select
        licenses.license_id,
        licenses.license_type,

    from licenses
    left join license_codes
        on licenses.license_type = license_codes.license_code
    where license_codes.license_code is null
)

select * from final
