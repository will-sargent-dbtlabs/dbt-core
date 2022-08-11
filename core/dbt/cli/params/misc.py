import click
from pathlib import Path, PurePath

config_dir = click.option(
    "--config-dir",
    help="If specified, DBT will show path information for this project",
    type=click.STRING,
)

skip_profile_setup = click.option(
    "--skip-profile-setup",
    "-s",
    help="Skip interative profile setup.",
    default=False,
)

output = click.option(
    "--output",
    help="TODO: No current help text",
    type=click.Choice(["json", "name", "path", "selector"], case_sensitive=False),
    default="name",
)

ouptut_keys = click.option(
    "--output-keys",
    help="TODO: No current help text",
    default=False,
)

show = click.option(
    "--show",
    help="Show a sample of the loaded data in the terminal",
    default=False,
)

output_path = click.option(
    "--output",
    "-o",
    help="Specify the output path for the json report. By default, outputs to 'target/sources.json'",
    type=click.Path(file_okay=True, dir_okay=False, writable=True),
    default=PurePath.joinpath(Path.cwd(), "target/sources.json"),
)

store_failures = click.option(
    "--store-failures", help="Store test results (failing rows) in the database", default=False
)
