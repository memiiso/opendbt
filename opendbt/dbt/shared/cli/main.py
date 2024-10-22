import click
from dbt.cli import requires, params as p
from dbt.cli.main import global_flags, cli

from opendbt.dbt.shared.task.sqlfluff import SqlFluffTasks


# dbt docs
@cli.group()
@click.pass_context
@global_flags
def sqlfluff(ctx, **kwargs):
    """Generate or serve the documentation website for your project"""


# dbt docs generate
@sqlfluff.command("lint")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.full_refresh
@p.indirect_selection
@p.profile
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.select
@p.selector
@p.show
@p.state
@p.defer_state
@p.deprecated_state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest(write=False)
def sqlfluff_lint(ctx, **kwargs):
    """Generate the documentation website for your project"""
    task = SqlFluffTasks(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.lint()
    success = task.interpret_results(results)
    return results, success


# dbt docs generate
@sqlfluff.command("fix")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.full_refresh
@p.indirect_selection
@p.profile
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.select
@p.selector
@p.show
@p.state
@p.defer_state
@p.deprecated_state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest(write=False)
def sqlfluff_fix(ctx, **kwargs):
    """Generate the documentation website for your project"""
    task = SqlFluffTasks(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.fix()
    success = task.interpret_results(results)
    return results, success
