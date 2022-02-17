with contacts as (
    select * from {{ ref('src_freshdesk_contacts') }}
),

profiles as (
    select * from {{ ref('util_user_profiles') }}
),

sectors as (
    select distinct
        sector_id,
        sector_name,
    from profiles
),

base_contacts as (
    select
        contacts.*,
        roles.role_name as role,
        sectors.sector_name as sector,

    from contacts
    left join sectors
        on contacts.sector_code = sectors.sector_id
    left join profiles as roles
        on contacts.role_code = roles.role_id
),

final as (
    select
        id,

        email,
        language,
        language_name,
        plan,
        subscription_period,
        country,
        country_name,
        sector_code,
        sector,
        role_code,
        role,
        city,
        n_total_creations,

        user_id,

        created_at,
        updated_at,
        subscription_started_at,
        last_login_at,
        registered_at

    from base_contacts
)

select * from final
