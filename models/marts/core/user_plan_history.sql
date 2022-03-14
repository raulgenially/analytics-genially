with user_history as (
    select
        id,
        user_id,
        subscription_plan as plan,
        state_valid_from as started_at,
        state_valid_to,
        registered_at

    from {{ ref('src_snapshot_genially_users') }}
),

compare_plan as (
    select
        *,
        lead(plan) over (
            partition by user_id order by started_at asc
        ) as next_plan,
        lag(plan) over (
            partition by user_id order by started_at asc
        ) as prev_plan

    from user_history
),

-- Here we are including:
-- - The first record (prev_plan is null)
-- - Records where a plan change occur (plan != prev_plan)
-- - The last record (next_plan is null)
-- We need the last record to extract the
-- end date of the currently active plan
compare_state as (
    select
        *,
        lead(state_valid_to) over (
            partition by user_id order by started_at asc
        ) as next_state_valid_to

    from compare_plan
    where plan != prev_plan
        or prev_plan is null
        or next_plan is null
),

-- Here we are including:
-- - The first record (prev_plan is null)
-- - Records where a plan change occur (plan != prev_plan)
-- We no longer need the last record (if have more than one
-- record with the same plan) because we already have
-- the end date of that record in next_state_valid_to
intermediate as (
    select
        *,
        lead(started_at) over (
            partition by user_id order by started_at asc
        ) as next_started_at

    from compare_state
    where plan != prev_plan
        or prev_plan is null
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
        case
            -- Record with currently active plan but it has
            -- other records with the same plan afterwards.
            -- Pick the end date of the truly last record.
            when next_started_at is null
                and next_state_valid_to is not null
                then next_state_valid_to
            -- Record with the currently active plan and it does
            -- not have other records with the same plan after it.
            -- Pick the end date of the current record.
            when next_started_at is null
                and next_state_valid_to is null
                then state_valid_to
            -- Intermediate record, let's not overlap with the next period.
            else timestamp_sub(next_started_at, interval 1 second)
        end as finished_at

    from intermediate
)

select * from final
