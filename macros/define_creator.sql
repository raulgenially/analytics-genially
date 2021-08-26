{% macro define_creator(min_creations=1) %}
    n_total_creations >= {{ min_creations }} or is_collaborator is true
{% endmacro %}
