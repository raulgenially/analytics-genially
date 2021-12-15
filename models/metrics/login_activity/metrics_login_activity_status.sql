with final as (
    {{ create_activity_status_model('status') }}
)

select * from final
