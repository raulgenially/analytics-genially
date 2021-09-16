with teams as (
    select * from {{ source('genially', 'team') }}
),

final as (
    select
        _id as team_id,

        name,
        seatsnumber as seats,
        logo,
        description,
        ifnull(type, 1) as team_type,
        banner,
        -- brandingsettings extraction
        json_extract_scalar(brandingsettings, '$.SizeWatermark') as branding_size_watermark,
        json_extract_scalar(brandingsettings, '$.CustomWatermark') as branding_custom_watermark,
        json_extract_scalar(brandingsettings, '$.WatermarkLink') as branding_watermark_link,
        json_extract_scalar(brandingsettings, '$.CustomLogo') as branding_custom_logo,

        iduser as owner_id,

        isconfigured as is_configured,

        creationtime as created_at,

    from teams
    where __hevo__marked_deleted = false
)

select * from final
