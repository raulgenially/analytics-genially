{% macro get_template_type_values() %}
    {{
        return([
            'template-presentation',
            'template-dossier',
            'template-reporting',
            'template-sales',
            'template-horizontal-infographic',
            'template-vertical-infographic',
            'template-diagrams',
            'template-quiz',
            'template-games',
            'template-escape-room',
            'template-video-presentation',
            'template-guide',
            'template-training-resources',
            'template-didactic-unit',
            'template-personal-branding',
            'template-headers',
            'template-post-h',
            'template-post-s',
            'template-post-v',
            'template-interactive-cards'
        ])
    }}
{% endmacro %}
