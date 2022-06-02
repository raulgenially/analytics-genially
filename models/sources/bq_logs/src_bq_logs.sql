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

with base_bq_logs as (
    select * from {{ source('bq_logs', 'cloudaudit_googleapis_com_data_access') }}
),

-- Here we remove rows when insertId column is duplicated
bq_logs as (
    {{ unique_records_by_column('base_bq_logs', 'insertId') }}
),

int_bq_logs as (
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

final as (
    select
        bq_logs_id,
        query,
        principal_email,
        referenced_tables,
        error_code,
        total_slots_ms,
        total_processed_bytes,
        total_billed_bytes,
        start_time,
        end_time,
        create_time

    from int_bq_logs
)

select * from final
