{% macro map_genially_category(template_type, genially_type) %}
    case 
        when {{ template_type }} = 'template-presentation'
            then 'Presentation'
        when {{ template_type }} in ('template-dossier', 'template-reporting', 'template-sales')
            then 'Dossier & Report'
         when {{ template_type }} in ('template-horizontal-infographic', 'template-diagrams')
            then 'Horizontal Infographic'
        when {{ template_type }} = 'template-vertical-infographic'
            then 'Vertical Infographic'
        when {{ template_type }} in ('template-quiz', 'template-games', 'template-escape-room')
            then 'Gamification'
        when {{ template_type }} = 'template-video-presentation'
            then 'Video presentation'
        when {{ template_type }} = 'template-guide'
            then 'Guide'
        when {{ template_type }} in ('template-training-resources', 'template-didactic-unit')
            then 'Training Material'
        when {{ template_type }} = 'template-personal-branding'
            then 'Personal Branding'
        when {{ template_type }} in ('template-headers', 'template-post-h', 'template-post-s', 'template-post-v', 'template-interactive-cards')
            then 'Social'
        when {{ template_type }} = 'template-hidden'
            then 'Template Hidden'
        when {{ genially_type }} = 17
            then 'Blank Creation'
        when {{ genially_type }} = 27
            then 'Interactive Image'
        when {{ genially_type }} = 18
            then 'Presentation (PPTX)'
        else
            'Other'
    end
{% endmacro %}