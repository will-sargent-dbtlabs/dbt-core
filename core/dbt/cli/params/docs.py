import click

compile = click.option(
    "--compile/--no-compile",
    help="Wether or not to run 'dbt compile' as part of docs generation",
    default=True,
)

port = click.option(
    "--port", help="Specify the port number for the docs server", default=8080, type=click.INT
)

browser = click.option(
    "--browser/--no-browser",
    help="Wether or not to open a local web browser after starting the server",
    default=True,
)
