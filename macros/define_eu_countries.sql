--This macro defines what countries are into European Union for billing purposes
{% macro define_eu_countries(country) %}

{{country}} in
('AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR','HR',
'HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK','GR')

{% endmacro %}
