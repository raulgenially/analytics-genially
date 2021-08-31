{% macro define_active_user(active_period=90) %}
    date(last_access_at) >= timestamp_sub(current_date(), interval {{ active_period }} day)
    or has_creation_visualized_last_{{ active_period }}_days
    or is_collaborator_of_creation_visualized_last_{{ active_period }}_days
{% endmacro %}
