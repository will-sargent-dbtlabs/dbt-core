from click import ParamType
import yaml


class YAML(ParamType):
    """The YAML type converts YAML strings into objects."""

    name = "YAML"

    def convert(self, value, param, ctx):
        # assume non-string values are a problem
        if not isinstance(value, str):
            self.fail(f"Cannot load YAML from type {type(value)}", param, ctx)
        try:
            return yaml.load(value, Loader=yaml.Loader)
        except yaml.parser.ParserError:
            self.fail(f"String '{value}' is not valid YAML", param, ctx)
