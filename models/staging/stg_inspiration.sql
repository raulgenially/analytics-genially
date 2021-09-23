with inspiration as (
    select * from {{ ref('src_genially_inspiration') }}
),

final as (
    select
        max(inspiration_id) as inspiration_id,
        max(tags) as tags,
        max(inspiration_order) as inspiration_order,
        max(name) as name,
        max(category) as category,
        max(language) as language,
        max(genially_type) as genially_type,
        max(is_reusable) as is_reusable,
        genially_id

    from inspiration
    group by 9
)

select * from final
