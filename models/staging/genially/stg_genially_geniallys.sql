with geniallys as (
    select
        _id as genially_id
        
    from {{ source('genially', 'geniallys') }}
)

select * from geniallys