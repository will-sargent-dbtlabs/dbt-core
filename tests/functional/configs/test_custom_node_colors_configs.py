import pytest

# from tests.functional.configs.fixtures import BaseConfigProject
from dbt.tests.util import run_dbt, get_manifest, write_file


models__custom_node_colors__model_sql = """
{{
    config(
        materialized='view',
        node_color='#c0c0c0'
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


# class TestCustomNodeColorConfigs(BaseConfigProject):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "models": {
#                 "test": {"+node_color": "#c0c0c0"},
#             },
#         }

#     def test_custom_color_layering(
#         self,
#         project,
#     ):
#         pass


# python3 -m pytest tests/functional/configs/test_custom_node_colors_configs.py
# TODO: node_color at model level creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well
class TestNodeColorConfigs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_yml

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": models__custom_node_colors__model_sql}

    def test_model_node_color_config(self, project):
        write_file(
            models__custom_node_colors__model_sql,
            project.project_root,
            "models",
            "my_model.sql",
        )
        run_dbt(["compile"])

        manifest = get_manifest(project.project_root)
        print(f"HELLO {manifest.nodes}")

        model_id = "model.test.my_model"
        model_node_config = manifest.nodes[model_id].config
        node_color_actual = model_node_config._extra["node_color"]

        node_color_expected = "#c0c0c0"

        assert node_color_actual == node_color_expected


# TODO: node_color in schema.yml overrides dbt_project.yml creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well

# TODO: node_color in a subfolder overrides global node_color in dbt_project.yml creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well

# test = {'model.test.my_model': ParsedModelNode(raw_sql="{{\n    config(\n        materialized='view',\n        node_color='#c0c0c0'\n    )\n}}\n\nselect 1 as id", database='dbt', schema='test16589609566819141019_test_custom_node_colors_configs', fqn=['test', 'my_model'], unique_id='model.test.my_model', package_name='test', root_path='/private/var/folders/hf/80s2zg792jv_n9rtdjt5rnkm0000gp/T/pytest-of-sung/pytest-34/project0', path='my_model.sql', original_file_path='models/my_model.sql', name='my_model', resource_type=<NodeType.Model: 'model'>, alias='my_model', checksum=FileHash(name='sha256', checksum='81429f3e51d581d912f072f258d79ca67bf0d9cd3495d9dceb02ba23c4d4cf0f'), config=NodeConfig(_extra={'node_color': '#c0c0c0'}, enabled=True, alias=None, schema=None, database=None, tags=[], meta={}, materialized='view', persist_docs={}, post_hook=[], pre_hook=[], quoting={}, column_types={}, full_refresh=None, unique_key=None, on_schema_change='ignore', grants={}, docs={'node_color': '#000000'}), _event_status={}, tags=[], refs=[], sources=[], metrics=[], depends_on=DependsOn(macros=[], nodes=[]), description='', columns={}, meta={}, docs={'node_color': '#000000'}, patch_path=None, compiled_path=None, build_path=None, deferred=False, unrendered_config={'docs': {'node_color': '#000000'}, 'materialized': 'view', 'node_color': '#c0c0c0'}, created_at=1658960957.997165, config_call_dict={'materialized': 'view', 'node_color': '#c0c0c0'})}

# "model.tpch.dim_customers": {
#       "raw_sql": "{{\n    config(\n        materialized = 'view',\n        transient=false,\n        node_color = 'pink'\n    )\n}}\n\n\nwith customer as (\n\n    select * from {{ ref('stg_tpch_customers') }}\n\n),\nnation as (\n\n    select * from {{ ref('stg_tpch_nations') }}\n),\nregion as (\n\n    select * from {{ ref('stg_tpch_regions') }}\n\n),\nfinal as (\n    select \n        customer.customer_key,\n        customer.name,\n        customer.address,\n        {# nation.nation_key as nation_key, #}\n        nation.name as nation,\n        {# region.region_key as region_key, #}\n        region.name as region,\n        customer.phone_number,\n        customer.account_balance,\n        customer.market_segment\n        -- new column\n    from\n        customer\n        inner join nation\n            on customer.nation_key = nation.nation_key\n        inner join region\n            on nation.region_key = region.region_key\n)\nselect \n    *\nfrom\n    final\norder by\n    customer_key",
#       "resource_type": "model",
#       "depends_on": {
#         "macros": [],
#         "nodes": [
#           "model.tpch.stg_tpch_customers",
#           "model.tpch.stg_tpch_nations",
#           "model.tpch.stg_tpch_regions"
#         ]
#       },
#   "config": {
#     "enabled": true,
#     "alias": null,
#     "schema": null,
#     "database": null,
#     "tags": [],
#     "meta": {},
#     "materialized": "view",
#     "persist_docs": {},
#     "quoting": {},
#     "column_types": {},
#     "full_refresh": null,
#     "unique_key": null,
#     "on_schema_change": "ignore",
#     "grants": {},
#     "docs": { "node_color": "#000000" },
#     "transient": false,
#     "node_color": "pink",
#     "post-hook": [],
#     "pre-hook": []
#   },
