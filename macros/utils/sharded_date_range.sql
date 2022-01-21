{% macro sharded_date_range(start_date, limit_data_on_dev=True, limit_days_on_dev=3) -%}

    {% set start_date = start_date if target.name == 'prod' or not limit_data_on_dev else (
            modules.datetime.date.today() - modules.datetime.timedelta(days=limit_days_on_dev)
        ).strftime('%Y-%m-%d')
    %}

    {% set end_date = (
            modules.datetime.date.today() - modules.datetime.timedelta(days=1)
        ).strftime('%Y-%m-%d')
    %}

    table_suffix >= format_date('%Y%m%d', date('{{ start_date }}'))
    and table_suffix < format_date('%Y%m%d', date('{{ end_date }}'))
{%- endmacro -%}
