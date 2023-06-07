{% set schema_names = [
    'google_sheets',
    'past_target'
] %}

{% set target_source = [] %}
{% for schema_name in schema_names %}
{% do target_source.append(source(schema_name,'daily_sale_targets')) %}
{% endfor %}

{{ dbt_utils.union_relations(target_source) }}
