import click

# Version is a special snowflake, list it first
version = click.option("--version", help="Show version information", is_flag=True, default=False)

# Global options that override config (sort alphabeticaly)
cache_selected_only = click.option(
    "--cache-selected-only/--no-cache-selected-only",
    help="Pre cache database objects relevant to selected resource only.",
    default=False,
)

debug = click.option(
    "--debug/--no-debug",
    "-d/ ",
    help="Display debug logging during dbt execution. Useful for debugging and making bug reports.",
    default=False,
)

fail_fast = click.option(
    "--fail-fast/--no-fail-fast", "-x/ ", help="Stop execution on first failure.", default=False
)

log_format = click.option(
    "--log-format",
    help="Specify the log format, overriding the command's default.",
    type=click.Choice(["text", "json", "default"], case_sensitive=False),
    default="default",
)

partial_parse = click.option(
    "--partial-parse/--no-partial-parse",
    help="Allow for partial parsing by looking for and writing to a pickle file in the target directory. This overrides the user configuration file.",
    default=True,
)

print = click.option(
    "--print/--no-print", help="Output all {{ print() }} macro calls.", default=True
)

printer_width = click.option(
    "--printer_width", help="Sets the width of terminal output", type=click.INT, default=80
)

quiet = click.option(
    "--quiet/--no-quiet",
    help="Suppress all non-error logging to stdout. Does not affect {{ print() }} macro calls.",
    default=False,
)

send_anonymous_usage_stats = click.option(
    "--anonymous-usage-stats/--no-anonymous-usage-stats",
    help="Send anonymous usage stats to dbt Labs.",
    default=True,
)

static_parser = click.option(
    "--static-parser/--no-static-parser", help="Use the static parser.", default=True
)

use_colors = click.option(
    "--use-colors/--no-use-colors",
    help="Output is colorized by default and may also be set in a profile or at the command line.",
    default=True,
)

use_experimental_parser = click.option(
    "--use-experimental-parser/--no-use-experimental-parser",
    help="Enable experimental parsing features.",
    default=False,
)

version_check = click.option(
    "--version-check/--no-version-check",
    help="Ensure dbt's version matches the one specified in the dbt_project.yml file ('require-dbt-version')",
    default=True,
)

warn_error = click.option(
    "--warn-error/--no-warn-error",
    help="If dbt would normally warn, instead raise an exception. Examples include --models that selects nothing, deprecations, configurations with no associated models, invalid test configurations, and missing sources/refs in tests.",
    default=False,
)

write_json = click.option(
    "--write-json/--no-write-json",
    help="Writing the manifest and run_results.json files to disk",
    default=True,
)


# Rarely used global options
event_buffer_size = click.option(
    "--event-buffer-size",
    help="Sets the max number of events to buffer in EVENT_HISTORY.",
    default=100000,
    type=click.INT,
)

record_timing = click.option(
    "-r",
    "--record-timing-info",
    help="When this option is passed, dbt will output low-level timing stats to the specified file. Example: `--record-timing-info output.profile`",
    is_flag=True,
    default=False,
)
