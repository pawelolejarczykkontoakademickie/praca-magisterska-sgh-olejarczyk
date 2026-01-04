--generate_alias_name.sql

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

  {%- set baza = custom_alias_name if custom_alias_name is not none else node.name -%}
  {%- set wariant = var('source_size', 'small') -%}
  {{ baza ~ '_' ~ wariant }}

{%- endmacro %}
