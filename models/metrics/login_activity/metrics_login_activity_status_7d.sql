with final as (
    {{ create_activity_status_model('status_7d') }}
)

select * from final
