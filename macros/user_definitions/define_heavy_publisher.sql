{% macro define_heavy_publisher(min_published_creations=5) %}
     (creations.n_published_creations
        + creations.n_published_creations_as_collaborator) >= {{ min_published_creations }}
     and {{ define_recurrence() }}
{% endmacro %}
