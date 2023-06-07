{% set lists = ['all','historical'] %}
{% set ledgers_source = [] %}
{% for list in lists %}
{% do ledgers_source.append(source('accounting'~'_'~list,'CA_BOM')) %}
{% endfor %}

{{ dbt_utils.union_relations(ledgers_source) }}
