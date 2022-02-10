with contacts as (
    select * from {{ source('freshdesk', 'contacts') }}
),

final as (
    select
        id,

        email,
        language,

        custom_fields.iduser as user_id,

        created_at,
        updated_at

    from contacts
)

select * from final
