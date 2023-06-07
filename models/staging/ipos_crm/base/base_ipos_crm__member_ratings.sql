{{
    config(
        enabled=false
    )
}}
{% set lists = ['bgn','5wine'] %}
{% set ledgers_source = [] %}
{% for list in lists %}
{% do ledgers_source.append(source('ipos_crm_'~list,'member_rating')) %}
{% endfor %}

{{ dbt_utils.union_relations(ledgers_source) }}
