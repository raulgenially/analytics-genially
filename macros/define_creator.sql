{% macro define_activated_user(min_creations=1) %}
    n_total_creations >= {{ min_creations }} or is_collaborator is true
{% endmacro %}
