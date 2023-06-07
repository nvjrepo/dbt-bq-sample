{% macro object_segment(objects='') -%}

        case 
            when {{ objects }}age < 18 then '<18'
            when {{ objects }}age >= 18 and {{ objects }}age < 22 then '18-22'
            when {{ objects }}age >= 22 and {{ objects }}age <= 27 then '22-27'
            when {{ objects }}age >= 28 and {{ objects }}age <= 32 then '28-32'
            when {{ objects }}age > 32 then '32+'
        end as age_group

{% endmacro %}