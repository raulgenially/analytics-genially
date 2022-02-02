-- Test that the dates present in the invoice description are correctly set
-- Sometimes the description is created manually and some errors are introduced
with invoices as (
    select * from {{ ref('stg_invoices') }}
    -- Invoices where description matches something like:
    -- 1 x Annual Student License Genial.ly (31 OCT 2021 - 31 OCT 2022)
    where regexp_contains(description, r'^\d+ x .* \(\d+ \w+ \d+ \- \d+ \w+ \d+\)')
),

final as (
    select
        *,
        safe.parse_date('%d %b %Y', regexp_extract(description, r'\((.*?) \- (?:.*?)\)')) as parsed_start_date,
        safe.parse_date('%d %b %Y', regexp_extract(description, r'\((?:.*?) \- (.*?)\)')) as parsed_end_date,
    from invoices
)

select * from final
where parsed_start_date is null
    or parsed_end_date is null
