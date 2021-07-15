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
        friendlyurl as friendly_url, -- url used for the view social

        ifnull(published, False) as is_published,
        ifnull(deleted, False) as is_deleted,
        ifnull(noindex, False) as is_private,
        ifnull(public, False) as is_password_free, -- TODO review the name
        ifnull(showinsocialprofile, False) as is_in_social_profile, -- concept different from view social
        ifnull(reusable, False) as is_reusable,
        ifnull(inspiration, False) as is_inspiration,

        iduser as user_id,
        idanalytics as analytics_id,
        reusedfrom as reused_from_id,        
        idfromtemplate as from_template_id,
        idteam as team_id,
        idteamtemplate as team_template_id,
        idfromteamtemplate as from_team_template_id,

        lastmodified as modified_at,
        -- First valid creation date is 2016-05-25T21:46:53 (as of 2021-07-15)
        if(creationtime >= '2016-05-25', creationtime, null) as created_at,

        /*-- First valid creation date is 2016-05-25T21:46:53 (as of 2021-07-15)
        case 
            when date(creationtime) < date(2016, 5, 25) or creationtime is null
                then '2016-01-01'
            else
                creationtime
        end as created_at,*/

        datepublished as published_at,
        lastview as last_view_at,
        datedeleted as deleted_at
   
    from geniallys
    where __hevo__marked_deleted = False
)

select * from final