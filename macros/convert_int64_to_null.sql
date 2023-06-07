{% macro convert_int64_to_null(schema_name,adds='') -%}
     case when safe_cast ({{ schema_name }} as int64) in(0) then null else cast ({{ schema_name }} as int64) end as {{ adds }}{{ schema_name }}
{%- endmacro -%}