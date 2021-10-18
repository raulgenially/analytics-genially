with geniallys as (
    select * from {{ source('genially', 'geniallys') }}
),

final as (
    select
        _id as genially_id,

        typegenially as genially_type,
        {{ map_subscription_code('typesubscription') }} as subscription_plan,
        name,
        tags,
        description,
        friendlyurl as friendly_url, -- url used for the social view

        ifnull(published, false) as is_published,
        not ifnull(deleted, false) and __hevo__marked_deleted = false as is_active,
        ifnull(deleted, false) and __hevo__marked_deleted = false and date_diff(current_timestamp(), datedeleted, day) < 30 as is_in_recyclebin,
        ifnull(deleted, false) and __hevo__marked_deleted = false and date_diff(current_timestamp(), datedeleted, day) >= 30 as is_logically_deleted,
        __hevo__marked_deleted as is_deleted,
        ifnull(noindex, false) as is_private,
        ifnull(public, false) as is_password_free,
        case
            when published = true and ifnull(noindex, false) = false
                then
                    case
                        when showinsocialprofile is null and reusable = true
                            then true
                        else ifnull(showinsocialprofile, false)
                    end
            else false
        end as is_in_social_profile, -- concept different from social view
        ifnull(reusable, false) as is_reusable,
        ifnull(inspiration, false) as is_inspiration,
        ifnull(hasgeniallyanalytics, false) as has_genially_analytics,

        iduser as user_id,
        idanalytics as analytics_id,
        reusedfrom as reused_from_id,
        idfromtemplate as from_template_id,
        idteam as team_id,
        idspace as space_id,
        idteamtemplate as team_template_id,
        idfromteamtemplate as from_team_template_id,

        lastmodified as modified_at,
        -- First valid creation date is 2016-05-25T21:46:53 (as of 2021-07-15)
        if(creationtime >= '2016-05-25', creationtime, null) as created_at,
        datepublished as published_at,
        lastview as last_view_at,
        datedeleted as deleted_at

    from geniallys
)

select * from final
