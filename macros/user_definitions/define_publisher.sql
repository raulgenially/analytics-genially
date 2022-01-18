{% macro define_publisher(min_published_creations=1) %}
    creations.n_published_creations >= {{ min_published_creations }}
    or creations.n_published_creations_as_collaborator >= {{ min_published_creations }}
{% endmacro %}
