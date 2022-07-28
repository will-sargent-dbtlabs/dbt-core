import pytest
import os

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

models__no_node_colors__model_sql = """
{{
    config(
        materialized='view'
    )
}}

select 1 as id

"""

models__custom_node_colors__schema_yml = """
version: 2

models:
  - name: my_model
    description: "This is a model description"
    config:
        node_color: 'pink'
"""

dbt_project_yml = """
models:
  test:
    +docs:
      node_color: "#000000"
    staging:
      +docs:
        node_color: "red"
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
class TestNodeColorConfigs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return dbt_project_yml

    def test_model_node_color_config(self, project):
        write_file(
            models__custom_node_colors__model_sql,
            project.project_root,
            "models",
            "my_model.sql",
        )
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        model_id = "model.test.my_model"
        model_node_config = manifest.nodes[model_id].config
        node_color_model_actual = model_node_config._extra["node_color"]
        node_color_root_actual = model_node_config.docs.node_color

        node_color_model_expected = "#c0c0c0"
        node_color_root_expected = "#000000"

        assert node_color_model_actual == node_color_model_expected
        assert node_color_root_actual == node_color_root_expected

    def test_schema_node_color_config(self, project):
        write_file(
            models__no_node_colors__model_sql,
            project.project_root,
            "models",
            "my_model.sql",
        )
        write_file(
            models__custom_node_colors__schema_yml,
            project.project_root,
            "models",
            "schema.yml",
        )
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        model_id = "model.test.my_model"
        model_node_config = manifest.nodes[model_id].config
        node_color_model_actual = model_node_config._extra["node_color"]
        node_color_root_actual = model_node_config.docs.node_color

        node_color_model_expected = "pink"
        node_color_root_expected = "#000000"

        assert node_color_model_actual == node_color_model_expected
        assert node_color_root_actual == node_color_root_expected

    def test_subfolder_node_color_config(self, project):
        model_dir = os.path.join(project.project_root, "models", "staging")
        os.makedirs(model_dir)
        write_file(
            models__no_node_colors__model_sql,
            model_dir,
            "my_subfolder_model.sql",
        )
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        print(f"manifest: {manifest.nodes}")

        model_id = "model.test.my_subfolder_model"
        model_node_config = manifest.nodes[model_id].config
        node_color_model_actual = model_node_config._extra
        node_color_root_actual = model_node_config.docs.node_color

        node_color_model_expected = {}
        node_color_root_expected = "red"

        assert node_color_model_actual == node_color_model_expected
        assert node_color_root_actual == node_color_root_expected


# TODO: node_color in a subfolder overrides global node_color in dbt_project.yml creates a docs object underneath config within the node manifest, and a root level docs object in the node manifest as well

# test = {'model.test.my_model': ParsedModelNode(raw_sql="{{\n    config(\n        materialized='view',\n        node_color='#c0c0c0'\n    )\n}}\n\nselect 1 as id", database='dbt', schema='test16589609566819141019_test_custom_node_colors_configs', fqn=['test', 'my_model'], unique_id='model.test.my_model', package_name='test', root_path='/private/var/folders/hf/80s2zg792jv_n9rtdjt5rnkm0000gp/T/pytest-of-sung/pytest-34/project0', path='my_model.sql', original_file_path='models/my_model.sql', name='my_model', resource_type=<NodeType.Model: 'model'>, alias='my_model', checksum=FileHash(name='sha256', checksum='81429f3e51d581d912f072f258d79ca67bf0d9cd3495d9dceb02ba23c4d4cf0f'), config=NodeConfig(_extra={'node_color': '#c0c0c0'}, enabled=True, alias=None, schema=None, database=None, tags=[], meta={}, materialized='view', persist_docs={}, post_hook=[], pre_hook=[], quoting={}, column_types={}, full_refresh=None, unique_key=None, on_schema_change='ignore', grants={}, docs={'node_color': '#000000'}), _event_status={}, tags=[], refs=[], sources=[], metrics=[], depends_on=DependsOn(macros=[], nodes=[]), description='', columns={}, meta={}, docs={'node_color': '#000000'}, patch_path=None, compiled_path=None, build_path=None, deferred=False, unrendered_config={'docs': {'node_color': '#000000'}, 'materialized': 'view', 'node_color': '#c0c0c0'}, created_at=1658960957.997165, config_call_dict={'materialized': 'view', 'node_color': '#c0c0c0'})}

# manifest = {'model.test.my_model': ParsedModelNode(raw_sql="{{\n    config(\n        materialized='view',\n        node_color='#c0c0c0'\n    )\n}}\n\nselect 1 as id", database='dbt', schema='test16590149696742425564_test_custom_node_colors_configs', fqn=['test', 'my_model'], unique_id='model.test.my_model', package_name='test', root_path='/private/var/folders/hf/80s2zg792jv_n9rtdjt5rnkm0000gp/T/pytest-of-sung/pytest-50/project0', path='my_model.sql', original_file_path='models/my_model.sql', name='my_model', resource_type=<NodeType.Model: 'model'>, alias='my_model', checksum=FileHash(name='sha256', checksum='81429f3e51d581d912f072f258d79ca67bf0d9cd3495d9dceb02ba23c4d4cf0f'), config=NodeConfig(_extra={'node_color': '#c0c0c0'}, enabled=True, alias=None, schema=None, database=None, tags=[], meta={}, materialized='view', incremental_strategy=None, persist_docs={}, post_hook=[], pre_hook=[], quoting={}, column_types={}, full_refresh=None, unique_key=None, on_schema_change='ignore', grants={}, docs=Docs(show=True, node_color='#000000')), _event_status={}, tags=[], refs=[], sources=[], metrics=[], depends_on=DependsOn(macros=[], nodes=[]), description='', columns={}, meta={}, docs=Docs(show=True, node_color='#000000'), patch_path=None, compiled_path=None, build_path=None, deferred=False, unrendered_config={'docs': {'node_color': '#000000'}, 'materialized': 'view', 'node_color': '#c0c0c0'}, created_at=1659014971.114518, config_call_dict={'materialized': 'view', 'node_color': '#c0c0c0'})}

# subfolder_manifest = {'model.test.my_model': ParsedModelNode(raw_sql="{{\n    config(\n        materialized='view'\n    )\n}}\n\nselect 1 as id", database='dbt', schema='test16590332893258542164_test_custom_node_colors_configs', fqn=['test', 'my_model'], unique_id='model.test.my_model', package_name='test', root_path='/private/var/folders/hf/80s2zg792jv_n9rtdjt5rnkm0000gp/T/pytest-of-sung/pytest-73/project0', path='my_model.sql', original_file_path='models/my_model.sql', name='my_model', resource_type=<NodeType.Model: 'model'>, alias='my_model', checksum=FileHash(name='sha256', checksum='96e7dcbd2f14d5f305dec7fd6f22cc46cdb07da2ae14787a7545470fc1e6324c'), config=NodeConfig(_extra={'node_color': 'pink'}, enabled=True, alias=None, schema=None, database=None, tags=[], meta={}, materialized='view', incremental_strategy=None, persist_docs={}, post_hook=[], pre_hook=[], quoting={}, column_types={}, full_refresh=None, unique_key=None, on_schema_change='ignore', grants={}, docs=Docs(show=True, node_color='#000000')), _event_status={}, tags=[], refs=[], sources=[], metrics=[], depends_on=DependsOn(macros=[], nodes=[]), description='This is a model description', columns={}, meta={}, docs=Docs(show=True, node_color='#000000'), patch_path='test://models/schema.yml', compiled_path=None, build_path=None, deferred=False, unrendered_config={'docs': {'node_color': '#000000'}, 'materialized': 'view'}, created_at=1659033290.666083, config_call_dict={'materialized': 'view'}), 'model.test.my_sub_folder_model': ParsedModelNode(raw_sql="{{\n    config(\n        materialized='view'\n    )\n}}\n\nselect 1 as id", database='dbt', schema='test16590332893258542164_test_custom_node_colors_configs', fqn=['test', 'staging', 'my_sub_folder_model'], unique_id='model.test.my_sub_folder_model', package_name='test', root_path='/private/var/folders/hf/80s2zg792jv_n9rtdjt5rnkm0000gp/T/pytest-of-sung/pytest-73/project0', path='staging/my_sub_folder_model.sql', original_file_path='models/staging/my_sub_folder_model.sql', name='my_sub_folder_model', resource_type=<NodeType.Model: 'model'>, alias='my_sub_folder_model', checksum=FileHash(name='sha256', checksum='96e7dcbd2f14d5f305dec7fd6f22cc46cdb07da2ae14787a7545470fc1e6324c'), config=NodeConfig(_extra={}, enabled=True, alias=None, schema=None, database=None, tags=[], meta={}, materialized='view', incremental_strategy=None, persist_docs={}, post_hook=[], pre_hook=[], quoting={}, column_types={}, full_refresh=None, unique_key=None, on_schema_change='ignore', grants={}, docs=Docs(show=True, node_color='red')), _event_status={}, tags=[], refs=[], sources=[], metrics=[], depends_on=DependsOn(macros=[], nodes=[]), description='', columns={}, meta={}, docs=Docs(show=True, node_color='red'), patch_path=None, compiled_path=None, build_path=None, deferred=False, unrendered_config={'docs': {'node_color': 'red'}, 'materialized': 'view'}, created_at=1659033290.957101, config_call_dict={'materialized': 'view'})}

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
