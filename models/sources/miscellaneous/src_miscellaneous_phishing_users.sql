with phishing as (
    select * from {{ source('miscellaneous', 'phishing_users') }}
),

final as (
    select
        user_id,
        report_date as reported_at

    from phishing
)

select * from final
