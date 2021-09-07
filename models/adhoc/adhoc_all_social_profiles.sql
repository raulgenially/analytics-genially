-- This query is used here:
-- https://docs.google.com/spreadsheets/d/1xrRQ6yJKbL3eB9g8sh_YzLsHGGMEi0B89TSfB9dl1T4

with users as (
    select * from {{ ref('users') }}
)

select
  users.email as UserName,
  CONCAT("https://view.genial.ly/profile/",users.social_profile_name) as URL,
  users.is_social_profile_active as active,
  users.sector,
  users.market,
  users.language,
  users.twitter_account,
  users.facebook_account,
  users.youtube_account,
  users.instagram_account,
  users.linkedin_account,
  users.about_me,
  users.n_active_creations_in_social_profile as geniallys_at_social_profile,
  users.n_active_reusable_creations_in_social_profile as reusable_geniallys_at_social_profile
from users
where users.social_profile_name is not null
