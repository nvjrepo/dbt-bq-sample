{% macro convert_string_to_number(schema_name) -%}
    case 
        when trim({{ schema_name }}) not in ('#DIV/0!','#VALUE!') then
            case
                when regexp_contains(trim({{ schema_name }}), r'%') then cast(replace(trim({{ schema_name }}),'%','') as float64)/100
                when trim({{ schema_name }}) like '%,%' then cast(replace(trim({{ schema_name }}),',','') as float64)
                when trim({{ schema_name }}) in ('-','') then 0
                else cast(trim({{ schema_name }}) as float64)
            end
        else 0
    end

{%- endmacro -%}