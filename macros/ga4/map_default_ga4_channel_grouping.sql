{%- macro map_default_ga4_channel_grouping(tsource, tmedium, tcampaign) -%}
    case
        when ({{ tsource }} in ('direct', '(direct)')  or {{ tsource }} is null)
            and (regexp_contains({{ tmedium }}, r'^(\(not set\)|\(none\))$')
                or {{ tmedium }} is null)
            then 'Direct'
        when regexp_contains({{ tcampaign }}, r'^(.*shop.*)$')
            and regexp_contains({{ tmedium }}, r'^(.*cp.*|ppc|paid.*)$')
            then 'Paid Shopping'
        when regexp_contains({{ tsource }}, r'^(google|bing)$')
            and regexp_contains({{ tmedium }}, r'^(.*cp.*|ppc|paid.*)$')
            then 'Paid Search'
        when (regexp_contains({{ tsource }}, r'^(twitter|facebook|fb|instagram|ig|linkedin|pinterest)$')
                and regexp_contains({{ tmedium }}, r'^(.*cp.*|ppc|paid.*|social_paid)$'))
             or regexp_contains(lower({{ tmedium }}), r'.*influencer.*')
            then 'Paid Social'
        when regexp_contains({{ tsource }}, r'^(youtube)$')
            and regexp_contains({{ tmedium }}, r'^(.*cp.*|ppc|paid.*)$')
            then 'Paid Video'
        when regexp_contains({{ tmedium }}, r'^(display|banner|expandable|interstitial|cpm)$')
            then 'Display'
        when regexp_contains({{ tmedium }}, r'^(.*cp.*|ppc|paid.*)$')
            then 'Paid Other'
        when regexp_contains({{ tmedium }}, r'^(.*shop.*)$')
            then 'Organic Shopping'
        when regexp_contains({{ tsource }}, r'^.*(twitter|t\.co|facebook|instagram|linkedin|lnkd\.in|pinterest).*')
            or regexp_contains({{ tmedium }}, r'^(social|social_advertising|social-advertising|social_network|social-network|social_media|social-media|sm|social-unpaid|social_unpaid)$')
            then 'Organic Social'
        when regexp_contains({{ tmedium }}, r'^(.*video.*)$')
            then 'Organic Video'
        when regexp_contains({{ tsource }}, r'^(google|bing|yahoo|baidu|duckduckgo|yandex|ask)$')
            or {{ tmedium }} = 'organic'
            then 'Organic Search'
        when regexp_contains(lower({{ tsource }}), r'^(email|mail|e-mail|e_mail|e mail|mail\.google\.com)$')
            or regexp_contains(lower({{ tmedium }}), r'^(email|mail|e-mail|e_mail|e mail)$')
            then 'Email'
        when regexp_contains({{ tmedium }}, r'^(affiliate|affiliates)$')
            then 'Affiliate'
        when lower({{ tmedium }}) = 'referral'
            then 'Referral'
        when {{ tmedium }} = 'audio'
            then 'Audio'
        when {{ tmedium }} = 'sms'
            then 'SMS'
        when ends_with({{ tmedium }}, 'push')
            or regexp_contains({{ tmedium }}, r'.*(mobile|notification).*')
            then 'Mobile Notification'
        else 'Other'
    end
{%- endmacro -%}
