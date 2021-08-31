{{
  config(
    materialized='view'
  )
}}

select * from {{ ref('users') }}
