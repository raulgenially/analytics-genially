with user_history as (
    select
        id,
        user_id,
        subscription_plan as plan,
        state_valid_from as valid_from,

    from {{ ref('src_snapshot_genially_users') }}
),

compare_next as (
    select
        id,
        user_id,
        plan,
        lead(plan) over (
            partition by user_id order by valid_from asc
        ) as next_plan,
        valid_from,

    from user_history
),

final as (
    select
        id,

        user_id,
        plan,
        {{ create_subscription_field('plan') }} as subscription,

        valid_from,
        ifnull(
            timestamp_sub(
                lead(valid_from) over (
                    partition by user_id order by valid_from asc
                ),
                interval 1 second
            ),
            timestamp("{{ var('the_distant_future') }}")
        ) as valid_to,

    from compare_next
    where plan != next_plan
        or next_plan is null
)

select * from final
