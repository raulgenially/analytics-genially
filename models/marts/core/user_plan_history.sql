with user_history as (
    select
        id,
        user_id,
        subscription_plan as plan,
        state_valid_from as started_at,
        registered_at

    from {{ ref('src_snapshot_genially_users') }}
),

compare_prev as (
    select
        id,
        user_id,
        lag(plan) over (
            partition by user_id order by started_at asc
        ) as prev_plan,
        plan,
        started_at,
        registered_at

    from user_history
),

final as (
    select
        id,

        user_id,
        plan,
        {{ create_subscription_field('plan') }} as subscription,

        -- Adjust started_at to the registration date on cases where the
        -- registration date and the snapshot happened closely.
        if(
            prev_plan is null
                and started_at > registered_at -- Guard against manually set registered_at in the future
                and datetime_diff(started_at, registered_at, hour) <= 25,
            registered_at,
            started_at
        ) as started_at,
        -- Don't overlap with the next period
        ifnull(
            timestamp_sub(
                lead(started_at) over (
                    partition by user_id order by started_at asc
                ),
                interval 1 second
            ),
            timestamp("{{ var('the_distant_future') }}")
        ) as finished_at

    from compare_prev
    where plan != prev_plan
        or prev_plan is null
)

select * from final
