{% macro drop_audit_dataset(dry_run=False) %}
    {% set drop_command -%}
        drop schema if exists `{{ target.database }}.dbt_test__audit` cascade;
    {%- endset %}

    {% do log(drop_command, True) %}
    {% if dry_run | as_bool == False and target.name == 'prod' %}
        {% do run_query(drop_command) %}
    {% endif %}

{%- endmacro -%}
