with final as (
    {{ create_activity_status_model('status_28d') }}
)

select * from final
