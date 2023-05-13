from pathlib import Path

import pytest
from dbt.tests.adapter.simple_seed.seeds import seeds__expected_sql
from dbt.tests.adapter.simple_seed.test_seed import (
    TestBasicSeedTests,
    TestSeedConfigFullRefreshOff,
    TestSeedConfigFullRefreshOn,
    TestSeedCustomSchema,
    TestSeedParsing,
    TestSeedSpecificFormats,
    TestSimpleSeedEnabledViaConfig,
    TestSimpleSeedWithBOM,
)
from dbt.tests.adapter.simple_seed.test_seed_type_override import (
    BaseSimpleSeedColumnOverride,
    seeds__disabled_in_config_csv,
    seeds__enabled_in_config_csv,
)
from dbt.tests.util import copy_file

fixed_setup_sql = seeds__expected_sql.replace("TIMESTAMP WITHOUT TIME ZONE", "DATETIME").replace(
    "TEXT", "VARCHAR(255)"
)

seeds__tricky_csv = """
seed_id,seed_id_str,a_bool,looks_like_a_bool,a_date,looks_like_a_date,relative,weekday
1,1,1,1,2019-01-01 12:32:30,2019-01-01 12:32:30,tomorrow,Saturday
2,2,1,1,2019-01-01 12:32:31,2019-01-01 12:32:31,today,Sunday
3,3,1,1,2019-01-01 12:32:32,2019-01-01 12:32:32,yesterday,Monday
4,4,0,0,2019-01-01 01:32:32,2019-01-01 01:32:32,tomorrow,Saturday
5,5,0,0,2019-01-01 01:32:32,2019-01-01 01:32:32,today,Sunday
6,6,0,0,2019-01-01 01:32:32,2019-01-01 01:32:32,yesterday,Monday
""".lstrip()

macros__schema_test = """
{% test column_type(model, column_name, type) %}

    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set col_type = col_types.get(column_name) %}
    {% set col_type = 'text' if col_type and 'character varying' in col_type else col_type %}

    {% set validation_message = 'Got a column type of ' ~ col_type ~ ', expected ' ~ type %}

    {% set val = 0 if col_type == type else 1 %}
    {% if val == 1 and execute %}
        {{ log(validation_message, info=True) }}
    {% endif %}

    select '{{ validation_message }}' as validation_error
    from (select 1 as empty) as nothing
    where {{ val }} = 1

{% endtest %}

"""

properties__schema_yml = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    tests:
    - column_type:
        type: date
  - name: seed_id
    tests:
    - column_type:
        type: text

- name: seed_tricky
  columns:
  - name: seed_id
    tests:
    - column_type:
        type: int
  - name: seed_id_str
    tests:
    - column_type:
        type: text
  - name: a_bool
    tests:
    - column_type:
        type: int
  - name: looks_like_a_bool
    tests:
    - column_type:
        type: text
  - name: a_date
    tests:
    - column_type:
        type: datetime
  - name: looks_like_a_date
    tests:
    - column_type:
        type: text
  - name: relative
    tests:
    - column_type:
        type: text
  - name: weekday
    tests:
    - column_type:
        type: text
"""


class TestSimpleSeedColumnOverrideSQLServer(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds__enabled_in_config_csv,
            "seed_disabled.csv": seeds__disabled_in_config_csv,
            "seed_tricky.csv": seeds__tricky_csv,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"schema_test.sql": macros__schema_test}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": properties__schema_yml,
        }


class TestBasicSeedTestsSQLServer(TestBasicSeedTests):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSeedConfigFullRefreshOnSQLServer(TestSeedConfigFullRefreshOn):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSeedConfigFullRefreshOffSQLServer(TestSeedConfigFullRefreshOff):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSeedCustomSchemaSQLServer(TestSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSimpleSeedEnabledViaConfigSQLServer(TestSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsingSQLServer(TestSeedParsing):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSimpleSeedWithBOMSQLServer(TestSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(fixed_setup_sql)
        copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


class TestSeedSpecificFormatsSQLServer(TestSeedSpecificFormats):
    pass
