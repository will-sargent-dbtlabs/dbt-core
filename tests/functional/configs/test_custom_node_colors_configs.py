import pytest

from tests.functional.configs.fixtures import BaseConfigProject
from dbt.tests.util import run_dbt, get_manifest


models__custom_node_colors__model_sql = """
{{
    config(
        materialized='view',
        node_color='#000000'
    )
}}

select 1 as id

"""

models__custom_node_colors__schema_yml = """
version: 2

models:
  - name: model
    description: "This is a model description"
    config:
        node_color: '#c0c0c0'
"""

dbt_project_yml = """
models:
  test:
    my_model:
      +docs:
        node_color: "#000000"
"""


class TestCustomNodeColorConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {"+node_color": "#c0c0c0"},
            },
        }

    def test_custom_color_layering(
        self,
        project,
    ):
        pass


# python3 -m pytest tests/functional/configs/test_custom_node_colors_configs.py
# TODO: node_color at model level creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well
class TestNodeColorConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": models__custom_node_colors__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_yml

    def test_model_node_color_config(self, project):
        # compile the project to generate the manifest
        run_dbt(["compile"])

        # get the manifest
        manifest = get_manifest(project.project_root)

        # get the node config for the model within the manifest
        model_id = "model.test.my_model"
        model_node_config = manifest.nodes[model_id]
        node_color_actual = model_node_config.config.docs["node_color"]

        node_color_expected = "#000000"

        # assert the node_color is as expected
        assert node_color_actual == node_color_expected


# TODO: node_color in schema.yml overrides dbt_project.yml creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well

# TODO: node_color in a subfolder overrides global node_color in dbt_project.yml creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well
