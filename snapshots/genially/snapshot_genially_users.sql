{% snapshot snapshot_genially_users %}

    {{
      config(
        unique_key='_id',
        strategy='timestamp',
        updated_at='etl_loaded_at',
        invalidate_hard_deleted=True
      )
    }}

    select
        *,
        timestamp_millis(__hevo__ingested_at) as etl_loaded_at

    from {{ source('genially', 'users') }}
    where __hevo__marked_deleted = false

{% endsnapshot %}
