with bq_logs as (
    select * from {{ ref('src_bq_logs') }}
),

int_bq_logs as (
    select
        bq_logs_id,
        query,
        case
            when regexp_contains(query, r'(?s)^SELECT \* FROM \(\nSELECT clmn[0-9_]+.*LIMIT 20000000|^SELECT t0\..*t0 LIMIT 100;|^SELECT t0[\._].* AS t0_.* LIMIT 100;')
                then "Data Studio"
            when (regexp_contains(query, r'^\/\* {"app": "dbt"')
                    or principal_email like '%dbt-analytics-prod%'
                    or principal_email like '%dbt-analytics-ci%')
                then "dbt"
            when (regexp_contains(query, r'.*__HEVO_QUERY_.*')
                    or principal_email like "%hevodata%")
                then "Hevo"
            when regexp_contains(query, r'^-- Metabase.*')
                then "Metabase"
            else "Other"
        end as query_source,
        principal_email,
        referenced_tables,

        coalesce(
            safe_divide(
                total_slots_ms,
                timestamp_diff(end_time, start_time, millisecond)
            )
        ) as avg_slots,
        total_processed_bytes,
        (total_billed_bytes / pow(2,30)) as total_billed_gigabytes,
        (total_billed_bytes / pow(2,40)) * 5 as estimated_cost_usd,

        coalesce(total_processed_bytes, total_slots_ms, error_code) is null
            as is_cached,
        coalesce(
            regexp_contains(query, 'cloudaudit_googleapis_com_data_access_'),
            false
        ) as is_audit_dashboard_query,
        error_code is not null as is_error,

        timestamp_diff(end_time, start_time, second) as runtime_secs,
        create_time

    from bq_logs
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
        create_time

    from int_bq_logs
)

select * from final
