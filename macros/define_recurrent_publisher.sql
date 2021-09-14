{% macro define_recurrent_publisher(min_published_creations=1) %}
    {{ define_publisher(min_published_creations) }}
    and date(last_access_at) > date(registered_at)
{% endmacro %}
