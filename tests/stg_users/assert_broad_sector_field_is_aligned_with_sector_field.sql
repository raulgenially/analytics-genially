-- Broad sector is aligned with sector
with users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        user_id,
        sector,
        broad_sector

    from users
    where (sector = '{{ var('not_selected') }}'
            and broad_sector != '{{ var('not_selected') }}')
        or (sector = '{{ var('unknown') }}'
            and broad_sector != '{{ var('unknown') }}')
)

select * from final
