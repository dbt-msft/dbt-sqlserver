import os

import pytest
from dbt.tests.adapter.simple_seed.fixtures import models__downstream_from_seed_actual
from dbt.tests.adapter.simple_seed.seeds import seed__actual_csv, seeds__expected_sql
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from dbt.tests.adapter.simple_seed.test_seed import TestBasicSeedTests as BaseBasicSeedTests
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedConfigFullRefreshOff as BaseSeedConfigFullRefreshOff,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedConfigFullRefreshOn as BaseSeedConfigFullRefreshOn,
)
from dbt.tests.adapter.simple_seed.test_seed import TestSeedCustomSchema as BaseSeedCustomSchema
from dbt.tests.adapter.simple_seed.test_seed import TestSeedParsing as BaseSeedParsing
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedSpecificFormats as BaseSeedSpecificFormats,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSimpleSeedEnabledViaConfig as BaseSimpleSeedEnabledViaConfig,
)
from dbt.tests.adapter.simple_seed.test_seed_type_override import (
    BaseSimpleSeedColumnOverride,
    seeds__disabled_in_config_csv,
    seeds__enabled_in_config_csv,
)
from dbt.tests.util import check_relations_equal, check_table_does_exist, get_connection, run_dbt

from dbt.adapters.sqlserver import SQLServerAdapter

fixed_setup_sql = seeds__expected_sql.replace(
    "TIMESTAMP WITHOUT TIME ZONE", "DATETIME2(6)"
).replace("TEXT", "VARCHAR(255)")

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
    {% set col_type = 'text' if col_type and 'varchar' in col_type else col_type %}

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
        type: datetime2
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


class TestBasicSeedTestsSQLServer(BaseBasicSeedTests):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)

    def test_simple_seed(self, project):
        """Build models and observe that run truncates a seed and re-inserts rows"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)

    def test_simple_seed_full_refresh_flag(self, project):
        """Drop the seed_actual table and re-create.
        Verifies correct behavior by the absence of the
        model which depends on seed_actual."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )


class TestSeedConfigFullRefreshOnSQLServer(BaseSeedConfigFullRefreshOn):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)

    def test_simple_seed_full_refresh_config(self, project):
        """Drop the seed_actual table and re-create.
        Verifies correct behavior by the absence of the
        model which depends on seed_actual."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )


class TestSeedConfigFullRefreshOffSQLServer(BaseSeedConfigFullRefreshOff):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSeedCustomSchemaSQLServer(BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSimpleSeedEnabledViaConfigSQLServer(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        adapter = project.adapter
        assert isinstance(project.adapter, SQLServerAdapter)
        with get_connection(project.adapter):
            rel = adapter.Relation.create(database=project.database, schema=project.test_schema)
            adapter.drop_schema(rel)


class TestSeedParsingSQLServer(BaseSeedParsing):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_setup_sql)


class TestSeedSpecificFormatsSQLServer(BaseSeedSpecificFormats):
    pass


class TestSeedBatchSizeMaxSQLServer(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {
            "five_columns.csv": """seed_id,first_name,email,ip_address,birthday
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57"""
        }

    def test_max_batch_size(self, project, logs_dir):
        run_dbt(["seed"])
        with open(os.path.join(logs_dir, "dbt.log"), "r") as fp:
            logs = "".join(fp.readlines())

        assert "Inserting batches of 400 records" in logs


class TestSeedBatchSizeCustomSQLServer(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {
            "six_columns.csv": """seed_id,first_name,last_name,email,ip_address,birthday
1,Larry,King,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,Perkins,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,Montgomery,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57"""
        }

    def test_custom_batch_size(self, project, logs_dir):
        run_dbt(["seed"])
        with open(os.path.join(logs_dir, "dbt.log"), "r") as fp:
            logs = "".join(fp.readlines())
        # this is changed from 350.
        # Fabric goes -1 of min batch of (2100/number of columns -1) or 400
        assert "Inserting batches of 349 records" in logs


class SeedConfigBase:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }


class SeedTestBase(SeedConfigBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        return {"seed_actual.csv": seed__actual_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models__downstream_from_seed_actual.sql": models__downstream_from_seed_actual,
        }

    def _build_relations_for_test(self, project):
        """The testing environment needs seeds and models to interact with"""
        seed_result = run_dbt(["seed"])
        assert len(seed_result) == 1
        check_relations_equal(project.adapter, ["seed_expected", "seed_actual"])

        run_result = run_dbt()
        assert len(run_result) == 1
        check_relations_equal(
            project.adapter, ["models__downstream_from_seed_actual", "seed_expected"]
        )

    def _check_relation_end_state(self, run_result, project, exists: bool):
        assert len(run_result) == 1
        check_relations_equal(project.adapter, ["seed_actual", "seed_expected"])
        if exists:
            check_table_does_exist(project.adapter, "models__downstream_from_seed_actual")
