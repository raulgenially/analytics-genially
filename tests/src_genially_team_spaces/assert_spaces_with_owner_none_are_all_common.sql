-- The spaces that have owner_id to null should all be the common space
with spaces as (
    select * from {{ ref('src_genially_team_spaces') }}
)

select
    *

from spaces
where spaces.is_common = false
    and spaces.owner_id is null
