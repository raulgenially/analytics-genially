-- To ensure that recurrent_publishers is a subset of publishers (there are no recurrent_publishers out of publishers)
with publishers as (
    select * from {{ ref('users') }}
    where is_publisher = true
),

recurrent_publishers as (
    select * from {{ ref('users') }}
    where is_recurrent_publisher = true
),

final as (
    select
        recurrent_publishers.user_id as recurrent_publishers_user_id,
        recurrent_publishers.email,
        publishers.user_id as publishers_user_id,

    from recurrent_publishers
    left join publishers
        on recurrent_publishers.user_id = publishers.user_id
    where publishers.user_id is null
)

select * from final
