import pytest
import os
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    read_file,
    relation_from_name,
    rm_file,
    write_file,
)

TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]

my_model_sql = """
  select 1 as fun
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["dbt_test_user_1"]
"""

user2_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["dbt_test_user_2"]
"""

class TestModelGrants:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql, "schema.yml": model_schema_yml}

    @pytest.fixture(scope="class", autouse=True)
    def get_test_users(self, project):
        test_users = []
        missing = []
        for env_var in TEST_USER_ENV_VARS:
            user_name = os.getenv(env_var)
            if not user_name:
                missing.append(env_var)
            else:
                test_users.append(user_name)
        if missing:
            pytest.skip(f"Test requires env vars with test users. Missing {', '.join(missing)}.")
        return test_users

    def test_basic(self, project, get_test_users, logs_dir):
        # Tests a project with a single model, view materialization
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        expected = {"select": ["dbt_test_user_1"]}
        assert model.config.grants == expected
        assert model.config.materialized == "view"

        log_contents = read_file(logs_dir, "dbt.log")
        my_model_relation = relation_from_name(project.adapter, "my_model")
        grant_log_line = f"grant select on table {my_model_relation} to dbt_test_user_1;"
        assert grant_log_line in log_contents

        # Switch to a different user, still view materialization
        rm_file(logs_dir, "dbt.log")
        write_file(user2_model_schema_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        log_contents = read_file(logs_dir, "dbt.log")
        print(log_contents)
        grant_log_line = f"grant select on table {my_model_relation} to dbt_test_user_2;"
        assert grant_log_line in log_contents
        # Note: We are not revoking grants here, so there is no revoke in the log


