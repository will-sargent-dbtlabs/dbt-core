import click
from dbt.cli.params import _global as global_params
from dbt.cli.params import docs as docs_params
from dbt.cli.params import misc as misc_params
from dbt.cli.params import parse as parse_params
from dbt.cli.params import project as project_params
from dbt.cli.params import run as run_params
import inspect  # This is temporary for RAT-ing
from pprint import pformat as pf
import sys


# dbt
@click.group(
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
@global_params.version
@global_params.cache_selected_only
@global_params.debug
@global_params.fail_fast
@global_params.log_format
@global_params.partial_parse
@global_params.print
@global_params.printer_width
@global_params.quiet
@global_params.send_anonymous_usage_stats
@global_params.static_parser
@global_params.use_colors
@global_params.use_experimental_parser
@global_params.version_check
@global_params.warn_error
@global_params.write_json
@global_params.event_buffer_size
@global_params.record_timing
def cli(ctx, **kwargs):
    """An ELT tool for managing your SQL transformations and data models.
    For more documentation on these commands, visit: docs.getdbt.com
    """
    if kwargs.get("version", False):
        click.echo(f"`version` called\n ctx.params: {pf(ctx.params)}")
        sys.exit()
    else:
        del ctx.params["version"]


# dbt build
@cli.command("build")
@click.pass_context
def build(ctx, **kwargs):
    """Run all Seeds, Models, Snapshots, and tests in DAG order"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt clean
@cli.command("clean")
@click.pass_context
@project_params.project_dir
@project_params.profiles_dir
@project_params.profile
@project_params.target
@project_params.vars
def clean(ctx, **kwargs):
    """Delete all folders in the clean-targets list (usually the dbt_packages and target directories.)"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt docs
@cli.group()
@click.pass_context
def docs(ctx, **kwargs):
    """Generate or serve the documentation website for your project"""


# dbt docs generate
@docs.command("generate")
@click.pass_context
@global_params.version_check
@project_params.project_dir
@project_params.profiles_dir
@project_params.profile
@project_params.target
@project_params.vars
@docs_params.compile
@run_params.defer
@run_params.threads
@run_params.target_path
@run_params.log_path
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
def docs_generate(ctx, **kwargs):
    """Generate the documentation website for your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.parent.params)}"
    )


# dbt docs serve
@docs.command("serve")
@click.pass_context
@project_params.project_dir
@project_params.profiles_dir
@project_params.profile
@project_params.target
@project_params.vars
@docs_params.port
@docs_params.browser
def docs_serve(ctx, **kwargs):
    """Serve the documentation website for your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.parent.params)}"
    )


# dbt compile
@cli.command("compile")
@click.pass_context
@global_params.version_check
@project_params.project_dir
@project_params.profiles_dir
@project_params.profile
@project_params.target
@project_params.vars
@run_params.parse_only
@run_params.threads
@run_params.target_path
@run_params.log_path
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
@run_params.defer
@run_params.full_refresh
def compile(ctx, **kwargs):
    """Generates executable SQL from source, model, test, and analysis files. Compiled SQL files are written to the target/ directory."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt debug
@cli.command("debug")
@click.pass_context
@global_params.version_check
@project_params.project_dir
@project_params.profiles_dir
@project_params.profile
@project_params.target
@project_params.vars
@misc_params.config_dir
def debug(ctx, **kwargs):
    """Show some helpful information about dbt for debugging. Not to be confused with the --debug option which increases verbosity."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt deps
@cli.command("deps")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
def deps(ctx, **kwargs):
    """Pull the most recent version of the dependencies listed in packages.yml"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt init
@cli.command("init")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@misc_params.skip_profile_setup
def init(ctx, **kwargs):
    """Initialize a new DBT project."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt list
# dbt TODO: Figure out aliasing for ls (or just c/p?)
@cli.command("list")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@misc_params.output
@misc_params.ouptut_keys
@run_params.resource_type
@run_params.models
@run_params.indirect_selection
@run_params.exclude
@run_params.selector
@run_params.state
def list(ctx, **kwargs):
    """List the resources in your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt parse
@cli.command("parse")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@parse_params.write_manifest
@parse_params.compile
@run_params.threads
@run_params.target_path
@run_params.log_path
@global_params.version_check
def parse(ctx, **kwargs):
    """Parses the project and provides information on performance"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt run
@cli.command("run")
@click.pass_context
@global_params.fail_fast
@global_params.version_check
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@run_params.log_path
@run_params.target_path
@run_params.threads
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
@run_params.defer
@run_params.full_refresh
def run(ctx, **kwargs):
    """Compile SQL and execute against the current target database."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt run operation
@cli.command("run-operation")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@run_params.args
def run_operation(ctx, **kwargs):
    """Run the named macro with any supplied arguments."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt seed
@cli.command("seed")
@click.pass_context
@global_params.version_check
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@run_params.full_refresh
@run_params.log_path
@run_params.target_path
@run_params.threads
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
@misc_params.show
def seed(ctx, **kwargs):
    """Load data from csv files into your data warehouse."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt snapshot
@cli.command("snapshot")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@run_params.threads
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
@run_params.defer
def snapshot(ctx, **kwargs):
    """Execute snapshots defined in your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt source
@cli.group()
@click.pass_context
def source(ctx, **kwargs):
    """Manage your project's sources"""


# dbt source freshness
@source.command("freshness")
@click.pass_context
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@run_params.threads
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
@misc_params.output_path  # TODO: Is this ok to re-use?  We have three different output params, how much can we consolidate?
def freshness(ctx, **kwargs):
    """Snapshots the current freshness of the project's sources"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.parent.params)}"
    )


# dbt test
@cli.command("test")
@click.pass_context
@global_params.fail_fast
@global_params.version_check
@misc_params.store_failures
@project_params.profile
@project_params.profiles_dir
@project_params.project_dir
@project_params.target
@project_params.vars
@run_params.indirect_selection
@run_params.log_path
@run_params.target_path
@run_params.threads
@run_params.models
@run_params.exclude
@run_params.selector
@run_params.state
@run_params.defer
def test(ctx, **kwargs):
    """Runs tests on data in deployed models. Run this after `dbt run`"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )
