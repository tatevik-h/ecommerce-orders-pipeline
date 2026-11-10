{#- Use the configured schema name as-is (marts, intermediate, reference)
    instead of dbt's default "<target_schema>_<custom_schema>" prefixing. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
