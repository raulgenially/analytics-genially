{% macro define_creator(min_creations=1) %}
    n_total_creations >= {{ min_creations }} or is_in_collaboration = true
{% endmacro %}
