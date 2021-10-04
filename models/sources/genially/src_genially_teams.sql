with src_teams as (
    select * from {{ source('genially', 'team') }}
),

team_types as (
    select * from {{ ref('team_type_codes') }}
),

-- sanitize team_type so that it always has a value
teams as (
    select
        *,
        ifnull(type, 1) as team_type

    from src_teams
),

final as (
    select
        teams._id as team_id,

        teams.name,
        teams.seatsnumber as seats,
        teams.logo,
        teams.description,
        teams.team_type,
        team_types.name as team_type_name,
        teams.banner,
        -- brandingsettings extraction
        json_extract_scalar(teams.brandingsettings, '$.SizeWatermark') as branding_size_watermark,
        json_extract_scalar(teams.brandingsettings, '$.CustomWatermark') as branding_custom_watermark,
        json_extract_scalar(teams.brandingsettings, '$.WatermarkLink') as branding_watermark_link,
        json_extract_scalar(teams.brandingsettings, '$.CustomLogo') as branding_custom_logo,

        teams.iduser as owner_id,

        ifnull(teams.isconfigured, false) as is_configured,

        teams.creationtime as created_at,

    from teams
    left join team_types
        on teams.team_type = team_types.code
    where __hevo__marked_deleted = false
)

select * from final