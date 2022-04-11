-- This macro performs the union of all users (active + deleted).
-- It's aimed to be used in different metrics models to keep the consistency across the full history.
{% macro create_unioned_all_users_model() %}

with final as (
    select
        user_id,
        plan,
        subscription,
        sector,
        broad_sector,
        role,
        broad_role,
        country,
        country_name,
        registered_at

    from {{ ref('users') }}

    union all

    select
        user_id,
        plan,
        subscription,
        sector,
        broad_sector,
        role,
        broad_role,
        country,
        country_name,
        registered_at

    from {{ ref('deleted_users') }}
)

select * from final

{% endmacro %}
