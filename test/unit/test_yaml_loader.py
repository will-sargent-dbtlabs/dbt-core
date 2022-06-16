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

class YamlLoadingUnitTest(unittest.TestCase):

    def test_load_yaml_anchors(self):
        profile_yml = load_yaml_text(profile_with_anchor)
        assert(profile_yml)
