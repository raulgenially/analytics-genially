with user_history as (
    select
        id,
        user_id,
        subscription_plan as plan,
        state_valid_from as started_at

    from {{ ref('src_snapshot_genially_users') }}
),

compare_next as (
    select
        id,
        user_id,
        plan,
        lead(plan) over (
            partition by user_id order by started_at asc
        ) as next_plan,
        started_at

    from user_history
),

final as (
    select
        id,

        user_id,
        plan,
        {{ create_subscription_field('plan') }} as subscription,

        started_at,
        ifnull(
            timestamp_sub(
                lead(started_at) over (
                    partition by user_id order by started_at asc
                ),
                interval 1 second
            ),
            timestamp("{{ var('the_distant_future') }}")
        ) as finished_at

    from compare_next
    where plan != next_plan
        or next_plan is null
)

select * from final
