with role_codes as (
    select * from {{ ref('role_codes') }}
),

sector_codes as (
    select * from {{ ref('sector_codes') }}
),

role_mapping as (
    select * from {{ ref('role_correspondence') }}
),

int_user_profile as (
    select
        role_codes.role_id,
        role_codes.role_name,
        role_codes.sector_id,
        sector_codes.sector_name,
        sector_codes.agg_sector

    from role_codes
    left join sector_codes
        on role_codes.sector_id = sector_codes.sector_id
),

role_correspondence as (
    select
        role_mapping.*,
        role_codes.role_name as new_role_name,
        role_codes.sector_id as new_sector_id,
        sector_codes.sector_name as new_sector_name,
        sector_codes.agg_sector

    from role_mapping
    left join role_codes
        on role_mapping.new_role_id = role_codes.role_id
    left join sector_codes
        on role_codes.sector_id = sector_codes.sector_id
),

final as (
    select
        int_user_profile.role_id,
        int_user_profile.role_name,
        int_user_profile.sector_id,
        int_user_profile.sector_name,
        ifnull(role_correspondence.new_role_id, int_user_profile.role_id) as new_role_id,
        ifnull(role_correspondence.new_role_name, int_user_profile.role_name) as new_role_name,
        ifnull(role_correspondence.new_sector_id, int_user_profile.sector_id) as new_sector_id,
        ifnull(role_correspondence.new_sector_name, int_user_profile.sector_name) as new_sector_name,
        ifnull(role_correspondence.agg_sector, int_user_profile.agg_sector) as agg_sector

    from int_user_profile
    left join role_correspondence
        on int_user_profile.role_id = role_correspondence.old_role_id
)

select * from final
