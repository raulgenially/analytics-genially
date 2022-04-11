with user_history as (
    select * from {{ ref('src_snapshot_genially_users') }}
),

phishing as (
    select * from {{ ref('src_miscellaneous_phishing_users') }}
),

final as (
    select
        user_history.*

    from user_history
    left join phishing
        on user_history.user_id = phishing.user_id
    where phishing.user_id is null
)

select * from final
