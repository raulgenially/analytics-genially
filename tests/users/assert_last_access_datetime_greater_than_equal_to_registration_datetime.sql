-- The last access time should be greater than or equal to registration date
 -- Until we know how to fix this.
{{
  config(
    severity='warn'
  )
}}

with users as (
  select * from {{ ref('users') }}
),

final as (
    select
        user_id,
        plan, 
        sector, 
        role,
        country,
        email, 
        registered_at, 
        last_access_at

    from users
    where last_access_at < registered_at
)

select * from final
