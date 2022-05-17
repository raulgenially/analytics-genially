{% set yesterday = "date_sub(current_date(), interval 1 day)" %}

with raw_events as (
    select * from {{ ref('src_snowplow_events') }}
    where date(event_received_at) >= date_sub(current_date(), interval 7 day)
        and (user_id is not null or network_user_id is not null)
        and date(event_received_at) <= {{ yesterday }}
),

modeled_events as (
    select * from {{ ref('seed_modeled_amplitude_events') }}
),

-- filter out events that are not modeled
raw_events_modeled as (
    select
        raw_events.*

    from raw_events
    inner join modeled_events
        on raw_events.event_name = modeled_events.event_name
),

raw_events_deduped as (
    {{
        unique_records_by_column(
            cte='raw_events_modeled',
            unique_column='event_id',
            order_by='event_triggered_at',
            dir='asc',
        )
    }}
),

countries as (
    select * from {{ ref('seed_country_codes') }}
),

users as (
    select * from {{ ref('users') }}
),

enriched_events as (
    select
        raw_events.* replace(
            case
                when event_name = 'page_view'
                    then 'page_viewed'
                else event_name
            end as event_name
        ),
        -- countries
        countries.name as geo_country_name,
        -- users
        users.sector_code as user_sector_code,
        nullif(users.sector,'{{ var('not_selected') }}') as user_sector,
        users.role_code as user_role_code,
        nullif(users.role,'{{ var('not_selected') }}') as user_role,
        nullif(users.country_name,'{{ var('not_selected') }}') as user_country,
        users.plan as user_plan,
        users.registered_at as user_registered_at,
        users.n_total_creations as user_creations,

    from raw_events_deduped as raw_events
    left join users
        on raw_events.user_id = users.user_id
    left join countries
        on raw_events.geo_country = countries.code
),

final as (
    select
        replace(initcap(event_name), '_',' ') as event_type,
        unix_millis(event_triggered_at) as time,
        user_id,
        network_user_id as device_id,
        event_id as insert_id,
        geo_city as city,
        geo_country_name as country,
        nullif(device_brand, 'Unknown') as device_brand,
        nullif(device_name, 'Unknown') as device_model,
        browser_language as language,
        geo_latitude as location_lat,
        geo_longitude as location_lng,
        operating_system as os_name,
        operating_system_version as os_version,
        geo_region_name as region,

        -- event_properties
        struct(
            session_id,
            session_count,
            page_url,
            page_title,
            page_referrer,
            page_urlpath as page_path,
            -- curated parameters
            regexp_extract(page_urlpath, r'^/teams/([^/]{24})/?') as team_id,
            regexp_extract(page_urlpath, r'(?:/(?:analytics|duplicate)|^/(?:editor|reuse))/([^/]*)/?$') as genially_id,
            regexp_extract(page_urlpath, r'^/usetemplate/([^/]*)/?') as template_id,
            regexp_extract(page_urlpath, r'/spaces/([^/]*)/?') as team_space_id,
            regexp_extract(page_urlpath, r'^/invoice/([^/]*)/?') as invoice_id,
            regexp_extract(page_urlpath, r'/folder/([^/]*)/?') as folder_id,
            regexp_extract(page_urlpath, r'/templates/([^/]*)/?') as template_section
        ) as event_properties,

        -- user_properties
        struct(
            user_country as country,
            user_sector_code as sector_id,
            user_sector as sector,
            user_role_code as role_id,
            user_role as role,
            format_timestamp("%Y-%m-%dT%X%Ez", user_registered_at) as registered_at,
            user_plan as plan,
            user_creations as n_creations
        ) as user_properties,

        -- Cursor used during ingestion.
        -- The events of yesterday are the ones that are going to be ingested on
        -- a given day so we have to bring its event_received_at to the present
        -- because the ingestion job loads events whose load time falls in the
        -- range (last_job_start_time, current_job_start_time].
        -- We add 5 min into the future so that no events are skipped if the table is
        -- being generated while the ingestion job runs.
        if(
            date(event_received_at) = {{ yesterday }},
            timestamp_add(current_timestamp(), interval 5 minute),
            event_received_at
        ) as event_received_at

    from enriched_events
)

select * from final
