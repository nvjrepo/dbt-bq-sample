{% set lists = ['all','baucat','historical'] %}
{% set ledgers_source = [] %}
{% for list in lists %}
{% do ledgers_source.append(source('accounting'~'_'~list,'WAREHOUSE_BALANCE_ACTUAL')) %}
{% endfor %}

{{ dbt_utils.union_relations(ledgers_source) }}
