{% set lists = ['current','historical'] %}
{% set ledgers_source = [] %}
{% for list in lists %}
{% do ledgers_source.append(source('demand_planning',list~'_demand_planning_fixed')) %}
{% endfor %}

{{ dbt_utils.union_relations(ledgers_source) }}
