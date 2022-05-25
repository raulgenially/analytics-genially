{% macro drop_audit_dataset(dry_run=False) %}
    {% set audit_dataset -%}
        {{ 'dbt_test__audit' if target.name == 'prod' else target.dataset }}
    {%- endset %}

    {% set drop_command -%}
        drop schema if exists `{{ target.database }}.{{ audit_dataset }}` cascade;
    {%- endset %}

    {% do log(drop_command, True) %}
    {% if dry_run | as_bool == False %}
        {% do run_query(drop_command) %}
    {% endif %}

{%- endmacro -%}
