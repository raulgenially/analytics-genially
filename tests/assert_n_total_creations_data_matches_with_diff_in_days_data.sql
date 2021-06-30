-- The info provided by the n_total_creations field should match 
--  with the info provided by adoption velocity

with final as (
    select
        user_id,
        n_total_creations,
        first_creation_at,
        last_creation_at,
        days_btw_registration_and_first_creation,
        days_btw_first_and_second_creation,
        days_btw_second_and_third_creation,
        days_btw_registration_and_third_creation

    from {{ ref('users') }}
    where n_total_creations = 0
            and (days_btw_registration_and_first_creation is not null
                or days_btw_first_and_second_creation is not null
                or days_btw_second_and_third_creation is not null
                or days_btw_registration_and_third_creation is not null
            )
        or n_total_creations = 1
            and (days_btw_first_and_second_creation is not null
                or days_btw_second_and_third_creation is not null
                or days_btw_registration_and_third_creation is not null
            )
        or n_total_creations = 2
            and (days_btw_second_and_third_creation is not null
                or days_btw_registration_and_third_creation is not null
            )
        or n_total_creations = 3
            and (days_btw_registration_and_first_creation is null
                or days_btw_first_and_second_creation is null
                or days_btw_second_and_third_creation is null
                or days_btw_registration_and_third_creation is null
            )

)

select * from final