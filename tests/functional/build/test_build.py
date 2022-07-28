import os

import pytest
import yaml
from dbt.tests.util import copy_file, run_dbt

from test.integration.base import normalize
from tests.functional.build.fixtures import (  # noqa: F401
    models,
    models_circular_relationship,
    models_failing,
    models_interdependent,
    models_simple_blocking,
    seeds,
    snapshots,
    tests_failing,
)


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
    def models(self, models):  # noqa: F811
        return {
            "model_2.sql": models["model_2.sql"],
            "test.yml": models["test.yml"],
            "model_0.sql": models["model_0.sql"],
            "model_1.sql": models["model_1.sql"],
            "model_99.sql": models["model_99.sql"],
        }

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
