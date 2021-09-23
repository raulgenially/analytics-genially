with signups as (
    select * from {{ ref('signups') }}
)

select
    email

from signups
-- The text after @ will be replaced by input from the user
where email like '%@genially.com'
