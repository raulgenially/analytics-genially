{% macro define_publisher(min_published_creations=1) %}
    n_published_creations >= {{ min_published_creations }} or is_in_collaboration_of_published_creation = true
{% endmacro %}
