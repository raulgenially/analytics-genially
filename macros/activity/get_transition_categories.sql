{% macro get_transition_categories() %}
    {{ return(['Activation', 'Inactivation', 'Retention', 'Churn', 'Resurrection', 'Hibernation']) }}
{% endmacro %}
