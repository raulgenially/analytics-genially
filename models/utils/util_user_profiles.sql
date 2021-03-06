with role_codes as (
    select * replace (
        if(version = 1, concat(role_name, ' (old)'), role_name) as role_name
    )

    from {{ ref('seed_role_codes') }}
),

sector_codes as (
    select * replace (
        if(version = 1, concat(sector_name, ' (old)'), sector_name) as sector_name
    )

    from {{ ref('seed_sector_codes') }}
),

role_correspondence as (
    select * from {{ ref('seed_role_correspondence') }}
),

int_user_profile as (
    select
        role_codes.role_id,
        role_codes.role_name,
        role_codes.sector_id,
        role_codes.broad_role,
        sector_codes.sector_name,
        sector_codes.broad_sector

    from role_codes
    left join sector_codes
        on role_codes.sector_id = sector_codes.sector_id
),

role_mapping as (
    select
        role_correspondence.*,
        role_codes.role_name as new_role_name,
        role_codes.broad_role,
        role_codes.sector_id as new_sector_id,
        sector_codes.sector_name as new_sector_name,
        sector_codes.broad_sector

    from role_correspondence
    left join role_codes
        on role_correspondence.new_role_id = role_codes.role_id
    left join sector_codes
        on role_codes.sector_id = sector_codes.sector_id
),

final as (
    select
        int_user_profile.role_id,
        int_user_profile.role_name,
        int_user_profile.sector_id,
        int_user_profile.sector_name,
        ifnull(role_mapping.new_role_id, int_user_profile.role_id) as new_role_id,
        ifnull(role_mapping.new_role_name, int_user_profile.role_name) as new_role_name,
        ifnull(role_mapping.broad_role, int_user_profile.broad_role) as broad_role,
        ifnull(role_mapping.new_sector_id, int_user_profile.sector_id) as new_sector_id,
        ifnull(role_mapping.new_sector_name, int_user_profile.sector_name) as new_sector_name,
        ifnull(role_mapping.broad_sector, int_user_profile.broad_sector) as broad_sector

    from int_user_profile
    left join role_mapping
        on int_user_profile.role_id = role_mapping.old_role_id
)

select * from final
