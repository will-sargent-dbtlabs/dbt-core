import click
from pathlib import Path, PurePath
from dbt.cli.params.types import YAML

project_dir = click.option(
    "--project-dir",
    help="Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
    default=Path.cwd(),
    type=click.Path(exists=True),
)

profiles_dir = click.option(
    "--profiles-dir",
    help=f"Which directory to look in for the profiles.yml file. Default = {PurePath.joinpath(Path.home(), '.dbt')}",
    default=PurePath.joinpath(Path.home(), ".dbt"),
    type=click.Path(
        exists=True,
    ),
)

profile = click.option(
    "--profile",
    help="Which profile to load. Overrides setting in dbt_project.yml.",
)

target = click.option("-t", "--target", help="Which target to load for the given profile")

# TODO validate the yaml (validation callback + custom type)
vars = click.option(
    "--vars",
    help="Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file. This argument should be a YAML string, eg. '{my_variable: my_value}'",
    type=YAML(),
)
