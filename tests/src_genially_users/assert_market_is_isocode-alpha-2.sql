-- The user countries are present in country_codes.csv
with country_codes as (
    select * from {{ ref('country_codes') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

final as (
    select
        users.user_id,
        users.email,
        users.country,

    from users
    left join country_codes
        on users.country = country_codes.code
    where users.country != '{{ var('not_selected') }}' and country_codes.code is null
)

select * from final
