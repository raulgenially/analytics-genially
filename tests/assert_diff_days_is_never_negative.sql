-- The difference in number of days should be never negative.

with final as (
    select
        user_id,
        first_creation_at,
        last_creation_at,
        days_btw_registration_and_first_creation,
        days_btw_first_and_second_creation,
        days_btw_second_and_third_creation,
        days_btw_registration_and_third_creation

    from {{ ref('users') }}
    where days_btw_registration_and_first_creation < 0
        or days_btw_first_and_second_creation < 0
        or days_btw_second_and_third_creation < 0
        or days_btw_registration_and_third_creation < 0
)

select * from final