-- Country name is "not-selected" when country code is "not-selected"
with users as (
    select * from {{ ref('users') }}
),

final as (
    select
        user_id,
        country,
        country_name

    from users
    where country = '{{ var('not_selected') }}'
        and country_name != '{{ var('not_selected') }}'
)

select * from final
