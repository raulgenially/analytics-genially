-- The user countries are present in country_codes.csv
{% set not_selected = 'Not-selected' %}

with country_codes as (
    select * from {{ ref('country_codes') }}
),

final as (
    select
        u.user_id,
        u.email,
        u.market,

    from {{ ref('users') }} as u
    left join country_codes as c
    on c.code = u.market
    where u.market != '{{ not_selected }}' and c.name is null
)

select * from final
