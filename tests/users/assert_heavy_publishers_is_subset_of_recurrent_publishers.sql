-- To ensure that heavy_publishers is a subset of recurrent_publishers (there are no heavy_publishers out of recurrent_publishers)
with recurrent_publishers as (
    select * from {{ ref('users') }}
    where is_recurrent_publisher = true
),

heavy_publishers as (
    select * from {{ ref('users') }}
    where is_heavy_publisher = true
),

final as (
    select
        heavy_publishers.user_id as heavy_publishers_user_id,
        heavy_publishers.email,
        recurrent_publishers.user_id as recurrent_publishers_user_id,

    from heavy_publishers
    left join recurrent_publishers
        on heavy_publishers.user_id = recurrent_publishers.user_id
    where recurrent_publishers.user_id is null
)

select * from final
