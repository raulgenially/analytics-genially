{% macro map_genially_category(template_type, genially_type) %}
    case 
        when {{ template_type }} = 'template-presentation'
            then 'Presentation'
        when {{ template_type }} in ('template-dossier', 'template-reporting', 'template-sales')
            then 'Dossier & Report'
        when {{ template_type }} in ('template-challenge', 'template-didactic-unit')
            then 'Learning Experience'
        when {{ template_type }} in ('template-breakout', 'template-game', 'template-game-action')
            then 'Gamification'
        when {{ template_type }} in ('template-diagrams', 'template-horizontal-lists', 'template-horizontal-timeline', 'template-maps', 'template-review')
            then 'Horizontal Infographic'
        when {{ template_type }} in ('template-vertical-generic', 'template-vertical-lists', 'template-vertical-timeline')
            then 'Vertical Infographic'
        when {{ template_type }} = 'template-guide'
            then 'Guide'
        when {{ template_type }} = 'template-video-presentation'
            then 'Video presentation'
        when {{ template_type }} = 'template-personal-branding'
            then 'Personal Branding'
        when {{ template_type }} in ('template-headers', 'template-post-h', 'template-post-s', 'template-post-v', 'template-social-action')
            then 'Social'
        when {{ template_type }} = 'template-hidden'
            then 'Template Hidden'
        when {{ genially_type }} = 17
            then 'Blank Creation'
        when {{ genially_type }} = 27
            then 'Interactive Image'
        else
             'Unknown'
    end
{% endmacro %}