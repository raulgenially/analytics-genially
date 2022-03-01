with contacts as (
    select * from {{ source('freshdesk', 'contacts') }}
),

countries as (
    select * from {{ ref('seed_country_codes') }}
),

languages as (
    select * from {{ ref('seed_freshdesk_language_codes') }}
),

base_contacts as (
    select
        *,
        {{ clean_country_code('custom_fields.country') }} as country_sanitized,
    from contacts
),

int_contacts as (
    select
        contacts.*,
        languages.name as language_name,
        countries.name as country_name

    from base_contacts as contacts
    left join languages
        on contacts.language = languages.code
    left join countries
        on contacts.country_sanitized = countries.code
),

final as (
    select
        id,

        email,
        language,
        language_name,
        case
            when custom_fields.plan = 'EDU TEAM'
                then 'EDU_TEAM'
            else custom_fields.plan
        end as plan,
        custom_fields.periodsubscription as subscription_period,
        country_sanitized as country,
        country_name,
        custom_fields.sector as sector_code,
        custom_fields.role as role_code,
        custom_fields.city,
        custom_fields.numberofgeniallys as n_total_creations,

        custom_fields.iduser as user_id,

        created_at,
        updated_at,
        custom_fields.startsubscriptiondate as subscription_started_at,
        custom_fields.lastloginat as last_login_at,
        custom_fields.registerdate as registered_at,

    from int_contacts
)

select * from final
