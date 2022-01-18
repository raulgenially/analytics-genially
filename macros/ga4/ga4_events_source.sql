{% macro ga4_events_source(start_date, event=null) %}
    with events as (
        select * from {{ source('analytics_261787761', 'events') }}
        where _table_suffix >= format_date('%Y%m%d',
            -- Limit the data in dev environments
            {% if target.name == 'prod' %}
                date('{{ start_date }}')
            {% else %}
                date_sub(current_date(), interval 2 day)
            {% endif %}
            )
            and _table_suffix < format_date('%Y%m%d', current_date())
        {% if event != null %}
            and event_name = '{{ event }}'
        {% endif %}
    ),

    final as (
        select
            event_name,
            -- event_params
            {{ ga4_param("event_params", "page_location", "string") }},
            {{ ga4_param("event_params", "page_title", "string") }},
            {{ ga4_param("event_params", "page_referrer", "string") }},
            {{ ga4_param("event_params", "loadingTime", "int", "loading_time") }},
            {{ ga4_param("event_params", "nconnectedusers", "int", "n_connected_users") }},
            {{ ga4_param("event_params", "data_tgm1", "string") }},
            {{ ga4_param("event_params", "data_tgm2", "string") }},
            {{ ga4_param("event_params", "data_tgm3", "string") }},
            -- user_properties
            {{ ga4_param("user_properties", "user_type", "string") }},
            {{ ga4_param("user_properties", "user_sector", "int") }},
            {{ ga4_param("user_properties", "user_role", "int") }},
            -- device
            device.category as device_category,
            device.mobile_brand_name,
            device.mobile_model_name,
            device.mobile_marketing_name,
            device.operating_system,
            device.operating_system_version,
            device.language,
            device.web_info.browser,
            device.web_info.browser_version,
            device.web_info.hostname,
            -- geo
            geo.continent,
            geo.country,
            geo.region,
            geo.city,
            geo.sub_continent,
            geo.metro as metro_area,
            -- traffic_source
            traffic_source.name as traffic_source_name,
            traffic_source.medium as traffic_source_medium,
            traffic_source.source as traffic_source,
            -- ecommerce
            ecommerce.transaction_id,
            {{ ga4_param("event_params", "currency", "string") }},
            {{ ga4_param("event_params", "affiliation", "string") }},
            {{ ga4_param("event_params", "coupon", "string") }},
            -- items
            items[safe_offset(0)].item_id,
            items[safe_offset(0)].item_name,
            items[safe_offset(0)].item_brand,
            items[safe_offset(0)].item_category,
            items[safe_offset(0)].price as item_price,

            user_id,
            user_pseudo_id,

            timestamp_micros(event_timestamp) as event_at,
            timestamp_micros(user_first_touch_timestamp) as first_touch_at,

        from events
    )

    select * from final
{% endmacro %}
