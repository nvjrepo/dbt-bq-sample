{% set lists = ['all','baucat'] %}
{% set ledgers_source = [] %}
{% for list in lists %}
{% do ledgers_source.append(source('accounting'~'_'~list,'ledgers_analytics')) %}
{% endfor %}

{{ dbt_utils.union_relations(ledgers_source) }}
