-- To ensure that promoters is a subset of publishers (there are no promoters out of publishers)
with publishers as (
  select * from {{ ref('publishers') }}
),

promoters as (
    select * from {{ ref('promoters') }}
),

final as (
    select
        promoters.user_id as promoters_user_id,
        promoters.email,
        publishers.user_id as publishers_user_id,
        publishers.email

    from promoters
    left join publishers
        on promoters.user_id = publishers.user_id
    where publishers.user_id is null
)

select * from final
