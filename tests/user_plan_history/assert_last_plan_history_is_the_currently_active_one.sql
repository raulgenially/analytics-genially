-- The last plan present in the plan_history model
-- should be the same than the plan in the users collection
with users as (
    select * from {{ ref('src_genially_users') }}
),

plan_history as (
    select * from {{ ref('user_plan_history') }}
),

active_plan as (
    {{
        unique_records_by_column(
            cte='plan_history',
            unique_column='user_id',
            order_by='started_at',
            dir='desc',
        )
    }}
),

final as (
    select
        users.user_id,
        users.subscription_plan,
        active_plan.plan as history_plan,
        active_plan.started_at,
        active_plan.finished_at

    from users
    left join active_plan
        on users.user_id = active_plan.user_id
    where users.subscription_plan != active_plan.plan
)

select * from final
