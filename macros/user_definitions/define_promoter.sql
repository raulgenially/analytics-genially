{% macro define_promoter(min_creations_in_social_profile=1) %}
    is_social_profile_active = true
    and (n_creations_in_social_profile >= {{ min_creations_in_social_profile }} or is_in_collaboration_of_creation_in_social_profile = true)
{% endmacro %}
