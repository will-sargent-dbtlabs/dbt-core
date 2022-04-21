import pytest

from dbt.tests.util import run_dbt

"""
from tests.functional.build_test.fixtures import (
    snapshots,
    tests_failing,
    models,
    models_failing,
    seeds,
    models_circular_relationship,
    models_simple_blocking,
    test_files,
    models_interdependent,
    project_files,
)  # noqa: F401
"""

# from test.integration.base import DBTIntegrationTest, use_profile, normalize
from test.integration.base import normalize
import yaml
import shutil
import os


class TestBuildBase:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["snapshots"],
            "seed-paths": ["seeds"],
            "seeds": {
                "quote_columns": False,
            },
        }

    def build(self, expect_pass=True, extra_args=None, **kwargs):
        args = ["build"]
        if kwargs:
            args.extend(("--args", yaml.safe_dump(kwargs)))
        if extra_args:
            args.extend(extra_args)

        return run_dbt(args, expect_pass=expect_pass)


class TestPassingBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models"

    def test__build_happy_path(
        self,
        project,
    ):
        self.build()


class TestFailingBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models-failing"

    def test__build_happy_path(
        self,
        project,
    ):
        results = self.build(expect_pass=False)
        assert len(results) == 13
        actual = [r.status for r in results]
        expected = ["error"] * 1 + ["skipped"] * 5 + ["pass"] * 2 + ["success"] * 5
        assert sorted(actual) == sorted(expected)


class TestFailingTestsBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "tests-failing"

    def test__failing_test_skips_downstream(
        self,
        project,
    ):
        results = self.build(expect_pass=False)
        assert len(results) == 13
        actual = [str(r.status) for r in results]
        expected = ["fail"] + ["skipped"] * 6 + ["pass"] * 2 + ["success"] * 4
        assert sorted(actual) == sorted(expected)


class TestCircularRelationshipTestsBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models-circular-relationship"

    def test__circular_relationship_test_success(
        self,
        project,
    ):
        """Ensure that tests that refer to each other's model don't create
        a circular dependency."""
        results = self.build()
        actual = [r.status for r in results]
        expected = ["success"] * 7 + ["pass"] * 2
        assert sorted(actual) == sorted(expected)


class TestSimpleBlockingTest(TestBuildBase):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models-simple-blocking"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["does-not-exist"],
            "seed-paths": ["does-not-exist"],
        }

    def test__simple_blocking_test(
        self,
        project,
    ):
        """Ensure that a failed test on model_a always blocks model_b"""
        results = self.build(expect_pass=False)
        actual = [r.status for r in results]
        expected = ["success", "fail", "skipped"]
        assert sorted(actual) == sorted(expected)


class TestInterdependentModels(TestBuildBase):
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
    def model_path(self):
        return "models-interdependent"

    def tearDown(self):
        if os.path.exists(normalize("models-interdependent/model_b.sql")):
            os.remove(normalize("models-interdependent/model_b.sql"))

    def test__interdependent_models(
        self,
        project,
    ):
        # check that basic build works
        shutil.copyfile("test-files/model_b.sql", "models-interdependent/model_b.sql")
        results = self.build()
        assert len(results) == 16

        # return null from model_b
        shutil.copyfile("test-files/model_b_null.sql", "models-interdependent/model_b.sql")
        results = self.build(expect_pass=False)
        assert len(results) == 16
        actual = [str(r.status) for r in results]
        expected = ["error"] * 4 + ["skipped"] * 7 + ["pass"] * 2 + ["success"] * 3
        assert sorted(actual) == sorted(expected)
