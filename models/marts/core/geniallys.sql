with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        genially_id,
        genially_type,
        genially_plan,
        is_published,
        is_deleted,
        is_private,
        is_password_free,
        is_in_social_profile,
        is_reusable,
        is_inspiration,
        reused_from_id,
        from_template_id,
        modified_at,
        created_at,
        published_at,
        last_view_at,
        deleted_at,

        genially_user_id as user_id,
        is_current_user,
        user_plan,
        user_sector,
        user_role,
        user_market

    from geniallys
)

select * from final