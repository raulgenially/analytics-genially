-- We shouldn't have overlap between plan periods within the same user.
with plan_history as (
    select * from {{ ref('user_plan_history') }}
),

preceding_plan_values as (
    select
    * ,
    lag(plan) over (
        partition by user_id
        order by started_at asc
    ) as preceding_plan,
    lag(finished_at) over (
        partition by user_id
        order by started_at asc
    ) as preceding_finished_at,

    from plan_history
),

final as (
    select * from preceding_plan_values
    where preceding_plan != plan
    and preceding_finished_at > started_at
)

select * from final
