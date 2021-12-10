{% snapshot snapshot_genially_users %}

    {{
      config(
        unique_key='_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deleted = True
      )
    }}

    select
        *,
        ifnull(lastaccesstime, dateregister) as updated_at

    from {{ source('genially', 'users') }}
    where __hevo__marked_deleted = false

{% endsnapshot %}
