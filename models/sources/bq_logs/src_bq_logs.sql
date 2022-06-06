{% set service_name = "'bigquery.googleapis.com'" %}
{% set method_name = "'jobservice.jobcompleted'" %}
{% set event_names %}
    (
        'table_copy_job_completed',
        'query_job_completed',
        'extract_job_completed',
        'load_job_completed'
    )
{% endset %}
{% set months = 12 %}
{% set price_per_tb = 5 %}

with raw_bq_logs as (
    select * from {{ source('bq_logs', 'cloudaudit_googleapis_com_data_access') }}
),

-- Here we remove rows when insertId column is duplicated
bq_logs as (
    {{ unique_records_by_column('raw_bq_logs', 'insertId') }}
),

base_bq_logs as (
    select
        insertId as bq_logs_id,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query as query,
        protopayload_auditlog.authenticationInfo.principalEmail as principal_email,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables
            as referenced_tables,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code as error_code,

        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs as total_slots_ms,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes
            as total_processed_bytes,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes
            as total_billed_bytes,

        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime as start_time,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime as end_time,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime as create_time

    from bq_logs
    where protopayload_auditlog.serviceName = {{ service_name }}
        and protopayload_auditlog.methodName = {{ method_name }}
        and protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName in {{ event_names }}
        and date_diff(
            current_date(),
            date(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime),
            month
        ) <= {{ months }}
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

        error_code,
        total_slots_ms,
        safe_divide(
            total_slots_ms,
            timestamp_diff(end_time, start_time, millisecond)
        ) as avg_slots,

        total_processed_bytes,
        total_billed_bytes,
        (total_billed_bytes / pow(2,30)) as total_billed_gigabytes,
        (total_billed_bytes / pow(2,40)) * {{ price_per_tb }}
            as estimated_cost_usd,

        coalesce(total_processed_bytes, total_slots_ms, error_code) is null
            as is_cached,
        coalesce(
            regexp_contains(query, 'cloudaudit_googleapis_com_data_access_'),
            false
        ) as is_audit_dashboard_query,
        error_code is not null as is_error,

        start_time,
        end_time,
        timestamp_diff(end_time, start_time, second) as runtime_secs,
        create_time

    from base_bq_logs
),

final as (
    select
        bq_logs_id,
        query,
        query_source,
        principal_email,
        referenced_tables,

        error_code,
        total_slots_ms,
        avg_slots,

        total_processed_bytes,
        total_billed_bytes,
        total_billed_gigabytes,
        estimated_cost_usd,

        is_cached,
        is_audit_dashboard_query,
        is_error,

        start_time,
        end_time,
        runtime_secs,
        create_time

    from int_bq_logs
)

select * from final
