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
    where (broad_sector = '{{ var('not_selected') }}'
            and sector != '{{ var('not_selected') }}')
        or (broad_sector != '{{ var('not_selected') }}'
            and sector = '{{ var('not_selected') }}')
)

select * from final
