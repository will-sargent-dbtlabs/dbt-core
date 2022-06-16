import pytest

from dbt.tests.util import run_dbt_and_capture, run_dbt
from dbt.exceptions import DuplicateYamlKeyException
from dbt.clients.yaml_helper import load_yaml_text

duplicate_key_schema__schema_yml = """
version: 2
models:
  - name: my_model
models:
  - name: my_model
"""

my_model_sql = """
  select 1 as fun
"""

duplicate_vars__dbt_project_yml = """
# dbt_project.yml

vars:
  foo: bar
  foo: bar
"""

my_model_vars_sql = """
  select '{{ var("foo") }}' as val
"""


class TestBasicDuplications:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": duplicate_key_schema__schema_yml,
        }

    def test_warning_in_stdout(self, project):
        results, stdout = run_dbt_and_capture(["run"])
        assert "Duplicate 'models' key found in yaml file models/schema.yml" in stdout

    def test_exception_is_raised_with_warn_error_flag(self, project):
        with pytest.raises(DuplicateYamlKeyException):
            run_dbt(["--warn-error", "run"])


class TestVarsDuplications:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return load_yaml_text(duplicate_vars__dbt_project_yml)

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model_vars.sql": my_model_vars_sql}

    def test_duplicate_vars_suceeds(self, project):
        run_dbt(["run"])
