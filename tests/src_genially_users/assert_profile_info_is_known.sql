-- The user profile info (sector and role) is known.
{{
  config(
    severity='warn' 
  )
}}

with users as (
    select * from {{ ref('src_genially_users') }}
),

final as (
    select
        user_id,
        sector, 
        sector_category,
        role,
        country,
        registered_at

    from users
    where sector = '{{ var('unknown') }}'
        or role = '{{ var('unknown') }}'
)

select * from final
