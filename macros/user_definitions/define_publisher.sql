{% macro define_publisher(min_published_creations=1) %}
    n_published_creations >= {{ min_published_creations }}
    or n_published_creations_as_collaborator >= 1
{% endmacro %}
