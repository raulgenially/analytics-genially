{% macro get_freshdesk_contact_plan_values() %}
    {{ return(['FREE', 'PRO', 'MASTER', 'EDU', 'EDU_TEAM', 'TEAM', 'STUDENT']) }}
{% endmacro %}
