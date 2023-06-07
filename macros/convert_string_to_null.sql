{% macro convert_string_to_null(schema_name,adds='') -%}
     case when lower(cast ({{ schema_name }} as string) ) in('nan','','none','0') then string(null) else cast ({{ schema_name }} as string) end as {{ adds }}{{ schema_name }}
{%- endmacro -%}