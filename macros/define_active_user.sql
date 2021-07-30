{% macro define_active_user(active_period=90) %}
    last_access_at > timestamp_sub(current_timestamp(), interval {{ active_period }} day)
{% endmacro %}
