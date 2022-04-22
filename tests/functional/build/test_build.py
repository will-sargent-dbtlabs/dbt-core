import pytest
import yaml
import os

from dbt.tests.util import run_dbt, copy_file
from dbt.tests.fixtures.project import write_project_files
from tests.functional.build.fixtures import (
    snapshots__snap_0_sql,
    snapshots__snap_1_sql,
    snapshots__snap_99_sql,
    tests_failing__model_2_sql,
    tests_failing__test_yml,
    tests_failing__model_0_sql,
    tests_failing__model_1_sql,
    tests_failing__model_99_sql,
    models__model_2_sql,
    models__test_yml,
    models__model_0_sql,
    models__model_1_sql,
    models__model_99_sql,
    seeds__countries_csv,
    models_circular_relationship__test_yml,
    models_circular_relationship__model_0_sql,
    models_circular_relationship__model_1_sql,
    models_circular_relationship__model_99_sql,
    models_simple_blocking__schema_yml,
    models_simple_blocking__model_b_sql,
    models_simple_blocking__model_a_sql,
    models_interdependent__schema_yml,
    models_interdependent__model_c_sql,
    models_interdependent__model_a_sql,
    models_failing__test_yml,
    models_failing__model_0_sql,
    models_failing__model_1_sql,
    models_failing__model_2_sql,
    models_failing__model_3_sql,
    models_failing__model_99_sql,
)
from test.integration.base import normalize


@pytest.fixture(scope="class")
def snapshots():
    return {
        "snap_1.sql": snapshots__snap_1_sql,
        "snap_0.sql": snapshots__snap_0_sql,
        "snap_99.sql": snapshots__snap_99_sql,
    }


@pytest.fixture(scope="class")
def tests_failing():
    return {
        "model_2.sql": tests_failing__model_2_sql,
        "test.yml": tests_failing__test_yml,
        "model_0.sql": tests_failing__model_0_sql,
        "model_1.sql": tests_failing__model_1_sql,
        "model_99.sql": tests_failing__model_99_sql,
    }


@pytest.fixture(scope="class")
def models():
    return {
        "model_2.sql": models__model_2_sql,
        "test.yml": models__test_yml,
        "model_0.sql": models__model_0_sql,
        "model_1.sql": models__model_1_sql,
        "model_99.sql": models__model_99_sql,
    }


@pytest.fixture(scope="class")
def models_failing():
    return {
        "model_3.sql": models_failing__model_3_sql,
        "model_2.sql": models_failing__model_2_sql,
        "test.yml": models_failing__test_yml,
        "model_0.sql": models_failing__model_0_sql,
        "model_1.sql": models_failing__model_1_sql,
        "model_99.sql": models_failing__model_99_sql,
    }


@pytest.fixture(scope="class")
def seeds():
    return {"countries.csv": seeds__countries_csv}


@pytest.fixture(scope="class")
def models_circular_relationship():
    return {
        "test.yml": models_circular_relationship__test_yml,
        "model_0.sql": models_circular_relationship__model_0_sql,
        "model_1.sql": models_circular_relationship__model_1_sql,
        "model_99.sql": models_circular_relationship__model_99_sql,
    }


@pytest.fixture(scope="class")
def models_simple_blocking():
    return {
        "schema.yml": models_simple_blocking__schema_yml,
        "model_b.sql": models_simple_blocking__model_b_sql,
        "model_a.sql": models_simple_blocking__model_a_sql,
    }


@pytest.fixture(scope="class")
def models_interdependent():
    return {
        "schema.yml": models_interdependent__schema_yml,
        "model_c.sql": models_interdependent__model_c_sql,
        "model_a.sql": models_interdependent__model_a_sql,
    }


@pytest.fixture(scope="class")
def project_files(
    project_root,
    snapshots,
    tests_failing,
    models,
    models_failing,
    seeds,
    models_circular_relationship,
    models_simple_blocking,
    models_interdependent,
):
    write_project_files(project_root, "snapshots", snapshots)
    write_project_files(project_root, "tests-failing", tests_failing)
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "models-failing", models_failing)
    write_project_files(project_root, "seeds", seeds)
    write_project_files(project_root, "models-circular-relationship", models_circular_relationship)
    write_project_files(project_root, "models-simple-blocking", models_simple_blocking)
    write_project_files(project_root, "models-interdependent", models_interdependent)


class BuildBase:
    def build(self, expect_pass=True, extra_args=None, **kwargs):
        args = ["build"]
        if kwargs:
            args.extend(("--args", yaml.safe_dump(kwargs)))
        if extra_args:
            args.extend(extra_args)

        return run_dbt(args, expect_pass=expect_pass)


class TestPassingBuild(BuildBase):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models"

    def test_build_happy_path(
        self,
        project,
    ):
        self.build(expect_pass=True)


class TestFailingBuild(BuildBase):
    @pytest.fixture(scope="class")
    def models(self, models_failing):  # noqa: F811
        return {
            "model_3.sql": models_failing["model_3.sql"],
            "model_2.sql": models_failing["model_2.sql"],
            "test.yml": models_failing["test.yml"],
            "model_0.sql": models_failing["model_0.sql"],
            "model_1.sql": models_failing["model_1.sql"],
            "model_99.sql": models_failing["model_99.sql"],
        }

    def test_build_sad_path(
        self,
        project,
    ):
        results = self.build(expect_pass=False)
        assert len(results) == 13
        actual = [r.status for r in results]
        expected = ["error"] * 1 + ["skipped"] * 5 + ["pass"] * 2 + ["success"] * 5
        assert sorted(actual) == sorted(expected)


class TestFailingTestsBuild(BuildBase):
    @pytest.fixture(scope="class")
    def models(self, tests_failing):  # noqa: F811
        return {
            "model_2.sql": tests_failing["model_2.sql"],
            "test.yml": tests_failing["test.yml"],
            "model_0.sql": tests_failing["model_0.sql"],
            "model_1.sql": tests_failing["model_1.sql"],
            "model_99.sql": tests_failing["model_99.sql"],
        }

    def test_failing_test_skips_downstream(
        self,
        project,
    ):
        results = self.build(expect_pass=False)
        assert len(results) == 13
        actual = [str(r.status) for r in results]
        expected = ["fail"] + ["skipped"] * 6 + ["pass"] * 2 + ["success"] * 4
        assert sorted(actual) == sorted(expected)


class TestCircularRelationshipTestsBuild(BuildBase):
    @pytest.fixture(scope="class")
    def models(self, models_circular_relationship):  # noqa: F811
        return {
            "test.yml": models_circular_relationship["test.yml"],
            "model_0.sql": models_circular_relationship["model_0.sql"],
            "model_1.sql": models_circular_relationship["model_1.sql"],
            "model_99.sql": models_circular_relationship["model_99.sql"],
        }

    def test_circular_relationship_test_success(
        self,
        project,
    ):
        """Ensure that tests that refer to each other's model don't create
        a circular dependency."""
        results = self.build()
        actual = [r.status for r in results]
        expected = ["success"] * 7 + ["pass"] * 2
        assert sorted(actual) == sorted(expected)


class TestSimpleBlockingTest(BuildBase):
    @pytest.fixture(scope="class")
    def models(self, models_simple_blocking):  # noqa: F811
        return {
            "schema.yml": models_simple_blocking["schema.yml"],
            "model_b.sql": models_simple_blocking["model_b.sql"],
            "model_a.sql": models_simple_blocking["model_a.sql"],
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["does-not-exist"],
            "seed-paths": ["does-not-exist"],
        }

    def test_simple_blocking_test(
        self,
        project,
    ):
        """Ensure that a failed test on model_a always blocks model_b"""
        results = self.build(expect_pass=False)
        actual = [r.status for r in results]
        expected = ["success", "fail", "skipped"]
        assert sorted(actual) == sorted(expected)


class TestInterdependentModels(BuildBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["snapshots-none"],
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self, models_interdependent):  # noqa: F811
        return {
            "schema.yml": models_interdependent["schema.yml"],
            "model_c.sql": models_interdependent["model_c.sql"],
            "model_a.sql": models_interdependent["model_a.sql"],
        }

    def tearDown(self):
        if os.path.exists(normalize("models-interdependent/model_b.sql")):
            os.remove(normalize("models-interdependent/model_b.sql"))

    def test_interdependent_models(self, project, test_data_dir):
        # check that basic build works
        copy_file(test_data_dir, "model_b.sql", project.project_root + "/models", ["model_b.sql"])
        results = self.build()
        assert len(results) == 16

        # return null from model_b
        copy_file(
            test_data_dir, "model_b_null.sql", project.project_root + "/models", ["model_b.sql"]
        )
        results = self.build(expect_pass=False)
        assert len(results) == 16
        actual = [str(r.status) for r in results]
        expected = ["error"] * 4 + ["skipped"] * 7 + ["pass"] * 2 + ["success"] * 3
        assert sorted(actual) == sorted(expected)
