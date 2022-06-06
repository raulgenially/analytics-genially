{{
    config(
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
            }
    )
}}

with bq_logs as (
    select * from {{ ref('src_bq_logs') }}
),

final as (
    select
        bq_logs_id,
        query,
        query_source,
        principal_email,
        referenced_tables,
        avg_slots,
        total_processed_bytes,
        total_billed_gigabytes,
        estimated_cost_usd,
        is_cached,
        is_audit_dashboard_query,
        is_error,
        runtime_secs,
        created_at

    from bq_logs
)

select * from final
