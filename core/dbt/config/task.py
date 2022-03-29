from argparse import Namespace
from ast import Str
from dataclasses import dataclass, field
from os import PathLike
from sys import argv
from typing import List, Union

import dbt.flags as flags

from .args import parse_args
from .profile import Profile, read_user_config
from .project import Project
from .renderer import DbtProjectYamlRenderer, ProfileRenderer
from .runtime import RuntimeConfig, UnsetProfileConfig


@dataclass
class TaskConfig:
    args: Union[Namespace, List]
    config: Union[RuntimeConfig, UnsetProfileConfig] = field(init=False)

    def __post_init__(self):

        # Get the args from the cli and "parse" them
        #TODO replace parse_args() with Click functionality
        self.args = parse_args(self.args) if self.args else parse_args(argv[1:])

        # Update flags
        # TODO: Flags are just env vars, replace with Click functionality
        user_config = read_user_config(flags.PROFILES_DIR)
        flags.set_from_args(self.args, user_config)
        
        # Generate a profile renderer
        _profile_renderer = ProfileRenderer()

        # Generate a profile
        _profile = Profile.render_from_args(
            self.args,
            _profile_renderer,
            "forced_deps_test"
        )

        # Generate a project renderer
        _project_renderer = DbtProjectYamlRenderer(_profile)

        # Generate a Project
        _project = Project.from_project_root(
            "/Users/iknox/Projects/dbt_projects/forced_deps/",
            _project_renderer,
            verify_version=bool(flags.VERSION_CHECK),
            )

        # Generate dependencies (often None)
        _dependencies = None

        # Generate a RuntimeConfig (this is only for certain types of tasks)
        self.config = RuntimeConfig.from_parts(
            _project,
            _profile,
            self.args,
            _dependencies,
        )
        breakpoint()