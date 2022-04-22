import pytest
from dbt.tests.fixtures.project import write_project_files


snapshots__snap_1_sql = """
{% snapshot snap_1 %}

{{
    config(
      target_database=database,
      target_schema=schema,
      unique_key='iso3',

      strategy='timestamp',
      updated_at='snap_1_updated_at',
    )
}}

SELECT
  iso3,
  "name",
  iso2,
  iso_numeric,
  cow_alpha,
  cow_numeric,
  fao_code,
  un_code,
  wb_code,
  imf_code,
  fips,
  geonames_name,
  geonames_id,
  r_name,
  aiddata_name,
  aiddata_code,
  oecd_name,
  oecd_code,
  historical_name,
  historical_iso3,
  historical_iso2,
  historical_iso_numeric,
  current_timestamp as snap_1_updated_at from {{ ref('model_1') }}

{% endsnapshot %}
"""

snapshots__snap_0_sql = """
{% snapshot snap_0 %}

{{
    config(
      target_database=database,
      target_schema=schema,
      unique_key='iso3',

      strategy='timestamp',
      updated_at='snap_0_updated_at',
    )
}}

select *, current_timestamp as snap_0_updated_at from {{ ref('model_0') }}

{% endsnapshot %}
"""

snapshots__snap_99_sql = """
{% snapshot snap_99 %}

{{
    config(
      target_database=database,
      target_schema=schema,
      strategy='timestamp',
      unique_key='num',
      updated_at='snap_99_updated_at',
    )
}}

select *, current_timestamp as snap_99_updated_at from {{ ref('model_99') }}

{% endsnapshot %}
"""

tests_failing__model_2_sql = """
{{ config(materialized='table') }}

select * from {{ ref('snap_1') }}
"""

tests_failing__test_yml = """
version: 2

models:
  - name: model_0
    columns:
      - name: iso3
        tests:
          - unique
          - not_null
      - name: historical_iso_numeric
        tests:
          - not_null
  - name: model_2
    columns:
      - name: iso3
        tests:
          - unique
          - not_null

"""

tests_failing__model_0_sql = """
{{ config(materialized='table') }}

select * from {{ ref('countries') }}
"""

tests_failing__model_1_sql = """
{{ config(materialized='table') }}

select * from {{ ref('snap_0') }}

"""

tests_failing__model_99_sql = """
{{ config(materialized='table') }}

select '1' as "num"
"""

models__model_2_sql = """
{{ config(materialized='table') }}

select * from {{ ref('snap_1') }}
"""

models__test_yml = """
version: 2

models:
  - name: model_0
    columns:
      - name: iso3
        tests:
          - unique
          - not_null
  - name: model_2
    columns:
      - name: iso3
        tests:
          - unique
          - not_null

"""

models__model_0_sql = """
{{ config(materialized='table') }}

select * from {{ ref('countries') }}
"""

models__model_1_sql = """
{{ config(materialized='table') }}

select * from {{ ref('snap_0') }}
"""

models__model_99_sql = """
{{ config(materialized='table') }}

select '1' as "num"
"""

models_failing__model_3_sql = """
{{ config(materialized='table') }}

select * from {{ ref('model_1') }}

"""

models_failing__model_2_sql = """
{{ config(materialized='table') }}

select * from {{ ref('snap_1') }}
"""

models_failing__test_yml = """
version: 2

models:
  - name: model_0
    columns:
      - name: iso3
        tests:
          - unique
          - not_null
  - name: model_2
    columns:
      - name: iso3
        tests:
          - unique
          - not_null

"""

models_failing__model_0_sql = """
{{ config(materialized='table') }}

select * from {{ ref('countries') }}
"""

models_failing__model_1_sql = """
{{ config(materialized='table') }}

select bad_column from {{ ref('snap_0') }}
"""

models_failing__model_99_sql = """
{{ config(materialized='table') }}

select '1' as "num"
"""

seeds__countries_csv = (
    """"iso3","name","iso2","iso_numeric","cow_alpha","cow_numeric","fao_code","un_code","wb_code","imf_code","fips","geonames_name","geonames_id","r_name","aiddata_name","aiddata_code","oecd_name","oecd_code","historical_name","historical_iso3","historical_iso2","historical_iso_numeric"
"ABW","Aruba","AW","533","","","","533","ABW","314","AA","Aruba","3577279","ARUBA","Aruba","12","Aruba","373","","","",""
"AFG","Afghanistan","AF","4","AFG","700","2","4","AFG","512","AF","Afghanistan","1149361","AFGHANISTAN","Afghanistan","1","Afghanistan","625","","","",""
"AGO","Angola","AO","24","ANG","540","7","24","AGO","614","AO","Angola","3351879","ANGOLA","Angola","7","Angola","225","","","",""
"AIA","Anguilla","AI","660","","","","660","AIA","312","AV","Anguilla","3573511","ANGUILLA","Anguilla","8","Anguilla","376","","","",""
"ALA","Aland Islands","AX","248","","","","248","ALA","","","Aland Islands","661882","ALAND ISLANDS","","","","","","","",""
"ALB","Albania","AL","8","ALB","339","3","8","ALB","914","AL","Albania","783754","ALBANIA","Albania","3","Albania","71","","","",""
"AND","Andorra","AD","20","AND","232","6","20","ADO","","AN","Andorra","3041565","ANDORRA","","","","","","","",""
"ANT","Netherlands Antilles","AN","530","","","","","ANT","353","NT","Netherlands Antilles","","NETHERLANDS ANTILLES","Netherlands Antilles","211","Netherlands Antilles","361","Netherlands Antilles","ANT","AN","530"
"ARE","United Arab Emirates","AE","784","UAE","696","225","784","ARE","466","AE","United Arab Emirates","290557","UNITED ARAB EMIRATES","United Arab Emirates","140","United Arab Emirates","576","","","","""
    ""
)

models_circular_relationship__test_yml = """
version: 2

models:
  - name: model_0
    columns:
      - name: iso3
        tests:
          - relationships:
              to: ref('model_1')
              field: iso3

  - name: model_1
    columns:
      - name: iso3
        tests:
          - relationships:
              to: ref('model_0')
              field: iso3

"""

models_circular_relationship__model_0_sql = """
{{ config(materialized='table') }}

select * from {{ ref('countries') }}
"""

models_circular_relationship__model_1_sql = """
{{ config(materialized='table') }}

select * from {{ ref('model_0') }}

"""

models_circular_relationship__model_99_sql = """
{{ config(materialized='table') }}

select '1' as "num"


"""

models_simple_blocking__schema_yml = """
version: 2

models:
  - name: model_a
    columns:
      - name: id
        tests:
          - not_null

"""

models_simple_blocking__model_b_sql = """
select * from {{ ref('model_a') }}

"""

models_simple_blocking__model_a_sql = """
select null as id

"""

models_interdependent__schema_yml = """
version: 2

models:
  - name: model_a
    columns:
      - name: id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('model_b')
              field: id
          - relationships:
              to: ref('model_c')
              field: id

  - name: model_b
    columns:
      - name: id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('model_a')
              field: id
          - relationships:
              to: ref('model_c')
              field: id

  - name: model_c
    columns:
      - name: id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('model_a')
              field: id
          - relationships:
              to: ref('model_b')
              field: id

"""

models_interdependent__model_c_sql = """
select * from {{ ref('model_b') }}

"""

models_interdependent__model_a_sql = """
select 1 as id

"""


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
