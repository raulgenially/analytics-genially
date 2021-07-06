-- Make sure the different types of templates are considered during the mapping of genially category
-- See macro "map_genially_category"

with geniallys as (
    select * from {{ ref('stg_geniallys') }}
),

final as (
    select
        genially_id,
        category, 
        from_template_id, 
        template_type, 
        template_name

    from geniallys
    where category = 'Other' and template_type is not null
)

select * from final