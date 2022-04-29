with events as (
    select
        *,
        contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)] as device,
        contexts_com_iab_snowplow_spiders_and_robots_1_0_0[safe_offset(0)] as bots

    from {{ source('rt_pipeline_prod1', 'events') }}
    where app_id != 'docs-example'
),

final as (
    select
        event_id,

        -- event properties
        event_name,
        app_id,
        page_url,
        page_title,
        page_urlhost,
        page_urlpath,
        page_urlquery,
        page_referrer,
        refr_urlhost,
        refr_urlpath,
        refr_urlquery,
        -- device
        device.device_class as device_category,
        device.device_brand as device_brand,
        device.device_name,
        device.operating_system_name as operating_system,
        device.operating_system_version,
        device.agent_language_code as browser_language_code,
        device.agent_language as browser_language,
        device.agent_name as browser_name,
        device.agent_name_version as browser_name_version,
        device.agent_name_version_major as browser_name_version_major,
        device.agent_version as browser_version,
        device.agent_version_major as browser_version_major,
        -- geo
        geo_country,
        geo_region,
        geo_region_name,
        geo_city,
        geo_latitude,
        geo_longitude,
        -- marketing
        mkt_medium as utm_medium,
        mkt_source as utm_source,
        mkt_campaign as utm_campaign,
        mkt_term as utm_term,
        mkt_content as utm_content,
        refr_medium,
        refr_source,
        refr_term,

        -- user identification
        user_id,
        domain_userid as domain_user_id,
        domain_sessionid as session_id,
        domain_sessionidx as session_count,
        network_userid as network_user_id,

        -- event timestamps
        collector_tstamp as event_received_at,
        derived_tstamp as event_triggered_at,
        load_tstamp as event_loaded_at

    from events
    where bots.spider_or_robot = false
)

select * from final
