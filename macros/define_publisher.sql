{% macro define_publisher(min_published_creations=1) %}
    n_active_published_creations >= {{ min_published_creations }} or is_collaborator_of_published_creation is true
{% endmacro %}
