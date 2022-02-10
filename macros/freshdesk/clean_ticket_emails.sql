{% macro clean_ticket_emails(email_column) %}
    array(
        select
            -- Some emails are given like this: premium@genial.ly
            -- While other emails are given like this: "GENIALLY WEB S.L." <premium@genial.ly>
            -- This regex extracts the email address
            lower(regexp_extract(e, r'([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)')),
        from unnest({{ email_column }}) as e
    ) as {{ email_column }}
{% endmacro %}
