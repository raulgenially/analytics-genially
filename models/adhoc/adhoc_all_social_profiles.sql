with users as (
    select * from {{ ref('users') }}
)

select
  email as UserName,
  CONCAT("https://view.genial.ly/profile/", social_profile_name) as URL,
  is_social_profile_active as active,
  sector,
  market,
  language,
  twitter_account,
  facebook_account,
  youtube_account,
  instagram_account,
  linkedin_account,
  about_me,
  n_active_creations_in_social_profile as geniallys_at_social_profile,
  n_active_reusable_creations_in_social_profile as reusable_geniallys_at_social_profile

from users
where social_profile_name is not null
order by email asc
