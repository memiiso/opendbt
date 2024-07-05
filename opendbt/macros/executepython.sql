{% materialization executepython, supported_languages=['python']%}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {% set grant_config = config.get('grants') %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}
  {{ run_hooks(pre_hooks) }}

  {% call noop_statement(name='main', message='Executed Python', code=compiled_code, rows_affected=-1, res=None) %}
      {%- set res = adapter.submit_local_python_job(model, compiled_code) -%}
  {% endcall %}
  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
