{% macro evangelist_clause(min_published_creations=1) %}
    n_published_creations >= {{ min_published_creations }}
{% endmacro %}
