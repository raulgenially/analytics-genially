{% macro define_creator(min_creations=1) %}
    creations.n_total_creations >= {{ min_creations }}
    or creations.is_collaborator = true
{% endmacro %}
