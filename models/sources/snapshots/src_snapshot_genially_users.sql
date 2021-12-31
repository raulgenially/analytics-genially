with users as (
    {{
        create_base_user_model(
            source_dataset='snapshots',
            source_table='snapshot_genially_users'
        )
    }}
),

final as (
    select
        dbt_scd_id as id,
        user_id,

        subscription_plan,
        sector_code,
        role_code,
        username,
        nickname,
        email_lower as email,
        country_code as country,
        country_name,
        city,
        logins,
        language,
        organization,
        facebook_account,
        twitter_account,
        youtube_account,
        instagram_account,
        linkedin_account,
        email_validation_token,
        about_me,
        row_number() over (
            partition by user_id
            order by dbt_valid_from
        ) as version,

        is_validated,
        dbt_valid_to is null as is_current_state,

        analytics_id,

        registered_at,
        last_access_at,
        email_validation_created_at,
        dbt_valid_from as state_valid_from,
        ifnull(
            dbt_valid_to,
            timestamp('{{ var('the_distant_future') }}')
        ) as state_valid_to

    from users
)

select * from final
