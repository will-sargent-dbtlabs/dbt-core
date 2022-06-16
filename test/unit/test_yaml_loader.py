from dbt.clients.yaml_helper import load_yaml_text
import unittest

profile_with_anchor = """
# profiles.yml

postgres:
  outputs:
    dev: &profile
      type: postgres
      host: localhost
      user: root
      password: password
      schema: public
      database: postgres
      port: 5432
      threads: 8
    prod:
      <<: *profile
    uat: *profile
  target: dev
"""

project_with_duped_var = """
# dbt_project.yml

vars:
  foo: bar
  foo: bar
"""

class YamlLoadingUnitTest(unittest.TestCase):

    def test_load_yaml_anchors(self):
        profile_yml = load_yaml_text(profile_with_anchor)
        assert(profile_yml)

    def test_load_duped_var(self):
        dbt_project_yml = load_yaml_text(project_with_duped_var)
        assert(dbt_project_yml) == {'var': {'foo': 'bar'}}
