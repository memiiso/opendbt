{% materialization executesql, supported_languages=['sql']%}

{#
  modified version of table materialization. it executes compiled sql statement as is.
#}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {% set grant_config = config.get('grants') %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {{ log(msg="Executing SQL: " ~ compiled_code ~ "", info=True) }}
  {% call statement('main', language=language, fetch_result=False) -%}
      {{ compiled_code }}
  {%- endcall %}

  {%- set result = load_result('main') -%}
    {{ log(msg="Execution result " ~ result ~ "", info=True) }}
 {# DISABLED
  {%- set result_data = result['data'] -%}
    {{ log(msg="Execution result_data " ~ result_data ~ "", info=True) }}
  {%- set result_status = result['response'] -%}
    {{ log(msg="Execution result_status " ~ result_status ~ "", info=True) }}
 END-DISABLED #}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
