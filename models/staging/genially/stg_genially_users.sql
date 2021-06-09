with users as (
    select 
        _id as user_id

    from {{ source('genially', 'users') }}
)

select * from users