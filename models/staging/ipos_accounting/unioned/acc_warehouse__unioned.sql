{% set lists = ['all','baucat','historical'] %}
{% set ledgers_source = [] %}
{% for list in lists %}
{% do ledgers_source.append(source('accounting'~'_'~list,'WAREHOUSE_ANALYTICS')) %}
{% endfor %}

{{ dbt_utils.union_relations(ledgers_source) }}
