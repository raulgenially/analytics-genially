with inspiration as (
    select * from {{ source('genially', 'inspirationv4') }}
),

final as (
    select
        _id as inspiration_id,

        tags,
        cast(inspiration.order as int64) as inspiration_order,
        name,
        category,
        language,
        typegenially as genially_type,

        ifnull(reusable, false) as is_reusable,

        geniallyid as genially_id,

    from inspiration
    where __hevo__marked_deleted = false
)

select * from final
