with contacts as (
    select * from {{ ref('src_freshdesk_contacts') }}
),

users as (
    select * from {{ ref('src_genially_users') }}
),

-- The user email is not unique.
-- Take the one that registered first.
dedup_users as (
    {{
        unique_records_by_column(
            cte='users',
            unique_column='email',
            order_by='registered_at',
            dir='asc',
        )
    }}
),

final as (
    select
        contacts.id,

        contacts.email,
        contacts.language,

        users.user_id,

        contacts.created_at,
        contacts.updated_at

    from contacts
    left join dedup_users as users
        on contacts.email = users.email
)

select * from final
