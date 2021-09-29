with signups as (
    select * from {{ ref('signups') }}
),

base as (
    select
        REGEXP_EXTRACT(email, r'@(.+)') as Domain,
        count(user_id) as Users,
        countif(plan != 'Free') as PremiumUsers,
        sum(n_total_creations) as Geniallys,

    from signups
    -- The text after @ and in between % will be replaced by input from the user
    where email like '%@%parisdescartes.fr%'
    group by Domain
    order by Users desc
),

final as (
    select
        *,
        ifnull(safe_divide(Geniallys, Users), 0) as GeniallysPerUser,
        ifnull(safe_divide(PremiumUsers, Users), 0) as Conversion,
        Users - PremiumUsers as ConversionPotential

    from base
)

select * from final
