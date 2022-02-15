with contacts as (
    select * from {{ source('freshdesk', 'contacts') }}
),

final as (
    select
        id,

        email,
        language,

        created_at,
        updated_at

    from contacts
)

select * from final
