{% macro define_promoter_user(min_published_creations=1) %}
    n_published_creations >= {{ min_published_creations }}
{% endmacro %}