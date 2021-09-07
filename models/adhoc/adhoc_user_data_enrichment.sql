with users as (
    select * from {{ ref('users') }}
)

select
    email,
    DATE(last_access_at) as last_access_at,
    plan,
    market,
    n_total_creations as total_creations

from users
-- These emails are used as an example. In the google sheet this is read from an input sheet
where email in ('luiscebrian@genially.com', 'antonio@genially.com', 'raul@genial.ly')
order by email asc
