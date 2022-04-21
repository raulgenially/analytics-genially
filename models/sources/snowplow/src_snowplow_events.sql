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
        refr_medium as utm_medium_referrer,
        refr_source as utm_source_referrer,
        refr_term as utm_term_referrer,
        -- ecommerce transaction
        tr_orderid as transaction_id,
        tr_total as transaction_total,
        tr_currency as transaction_currency,
        tr_affiliation as transaction_affiliation,
        -- ecommerce item
        ti_orderid as item_order_id,
        ti_sku as item_id,
        ti_name as item_name,
        ti_category as item_category,
        ti_price as item_price,
        ti_quantity as item_quantity,
        ti_currency as item_currency,

        -- user identification
        user_id,
        domain_userid as domain_user_id,
        domain_sessionid as domain_session_id,
        domain_sessionidx as domain_session_idx,
        network_userid as network_user_id,

        -- event timestamps
        collector_tstamp as event_received_at,
        dvce_created_tstamp as event_triggered_at,
        load_tstamp as event_loaded_at

    from events
    where bots.spider_or_robot = false
)

select * from final
