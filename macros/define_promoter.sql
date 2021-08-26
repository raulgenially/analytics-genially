{% macro define_promoter(min_creations_in_social_profile=1) %}
    n_creations_in_social_profile >= {{ min_creations_in_social_profile }} or is_collaborator_of_creation_in_social_profile = true
{% endmacro %}
