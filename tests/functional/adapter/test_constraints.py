from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)


class TestModelConstraintsRuntimeEnforcementSQLServer(
    BaseModelConstraintsRuntimeEnforcement,
):
    pass


class TestTableConstraintsColumnsEqualSQLServer(BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqualSQLServer(BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqualSQLServer(
    BaseIncrementalConstraintsColumnsEqual,
):
    pass


class TestTableConstraintsRuntimeDdlEnforcementSQLServer(
    BaseConstraintsRuntimeDdlEnforcement,
):
    pass


class TestTableConstraintsRollbackSQLServer(BaseConstraintsRollback):
    pass


class TestIncrementalConstraintsRuntimeDdlEnforcementSQLServer(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


import re

import pytest

from dbt.tests.adapter.constraints.fixtures import (
    foreign_key_model_sql,
    model_data_type_schema_yml,
    my_incremental_model_sql,
    my_model_data_type_sql,
    my_model_incremental_with_nulls_sql,
    my_model_incremental_wrong_name_sql,
    my_model_incremental_wrong_order_depends_on_fk_sql,
    my_model_incremental_wrong_order_sql,
    my_model_sql,
    my_model_view_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_with_nulls_sql,
    my_model_wrong_name_sql,
    my_model_wrong_order_depends_on_fk_sql,
    my_model_wrong_order_sql,
)
from dbt.tests.util import (
    get_manifest,
    read_file,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
        name: pk_my_model_pk
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: unique
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
        name: pk_my_model_pk
      - type: unique
        columns: [id]
        name: uk_my_model_pk
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
"""

model_fk_constraint_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
        name: pk_my_model_pk
      - type: foreign_key
        expression: {schema}.foreign_key_model (id)
        name: fk_my_model_id
        columns: [id]
      - type: unique
        name: uk_my_model_id
        columns: [id]
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [id]
        name: pk_my_ref_model_id
      - type: unique
        name: uk_my_ref_model_id
        columns: [id]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
"""

constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: check
        expression: id >= 1
      - type: primary_key
        columns: [ id ]
        name: strange_pk_requirement_my_model
      - type: unique
        columns: [ color, date_day ]
        name: strange_uniqueness_requirement_my_model
      - type: foreign_key
        columns: [ id ]
        expression: {schema}.foreign_key_model (id)
        name: strange_pk_fk_requirement_my_model
    columns:
      - name: id
        data_type: int
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: color
        data_type: varchar(100)
      - name: date_day
        data_type: varchar(100)
  - name: foreign_key_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        columns: [ id ]
        name: strange_pk_requirement_fk_my_model
      - type: unique
        columns: [ id ]
        name: fk_id_uniqueness_requirement
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
"""


def _normalize_whitespace(input: str) -> str:
    subbed = re.sub(r"\s+", " ", input)
    return re.sub(r"\s?([\(\),])\s?", r"\1", subbed).lower().strip()


def _find_and_replace(sql, find, replace):
    sql_tokens = sql.split()
    for idx in [n for n, x in enumerate(sql_tokens) if find in x]:
        sql_tokens[idx] = replace
    return " ".join(sql_tokens)


class BaseConstraintsColumnsEqual:
    """dbt should catch these mismatches during its "preflight" checks."""

    @pytest.fixture()
    def string_type(self):
        return "varchar"

    @pytest.fixture()
    def int_type(self):
        return "int"

    @pytest.fixture()
    def schema_string_type(self, string_type):
        return string_type

    @pytest.fixture()
    def schema_int_type(self, int_type):
        return int_type

    @pytest.fixture()
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "date"],
            ["cast(1 as bit)", "bit", "bit"],
            [
                "cast('2013-11-03 00:00:00.000000' as datetime2(6))",
                "datetime2(6)",
                "datetime2(6)",
            ],
            ["cast(1 as decimal(5,2))", "decimal", "decimal"],
        ]

    def test__constraints_wrong_column_order(self, project):
        # This no longer causes an error, since we enforce yaml column order
        run_dbt(["run", "-s", "my_model_wrong_order"], expect_pass=True)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_order"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

    def test__constraints_wrong_column_names(self, project, string_type, int_type):
        _, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_name"],
            expect_pass=False,
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

        expected = ["id", "error", "missing in definition", "missing in contract"]
        assert all(
            [(exp in log_output or exp.upper() in log_output) for exp in expected]
        )

    def test__constraints_wrong_column_data_types(
        self,
        project,
        string_type,
        int_type,
        schema_string_type,
        schema_int_type,
        data_types,
    ):
        for sql_column_value, schema_data_type, error_data_type in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )

            # Write wrong data_type to corresponding schema file
            # Write integer type for all schema yaml values except when testing integer type itself
            wrong_schema_data_type = (
                schema_int_type
                if schema_data_type.upper() != schema_int_type.upper()
                else schema_string_type
            )
            wrong_schema_error_data_type = (
                int_type
                if schema_data_type.upper() != schema_int_type.upper()
                else string_type
            )
            write_file(
                model_data_type_schema_yml.format(data_type=wrong_schema_data_type),
                "models",
                "constraints_schema.yml",
            )

            results, log_output = run_dbt_and_capture(
                ["run", "-s", "my_model_data_type"],
                expect_pass=False,
            )
            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config.enforced is True
            expected = [
                "wrong_data_type_column_name",
                error_data_type,
                wrong_schema_error_data_type,
                "data type mismatch",
            ]
            assert all(
                [(exp in log_output or exp.upper() in log_output) for exp in expected]
            )

    def test__constraints_correct_column_data_types(self, project, data_types):
        for sql_column_value, schema_data_type, _ in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )
            # Write correct data_type to corresponding schema file
            write_file(
                model_data_type_schema_yml.format(data_type=schema_data_type),
                "models",
                "constraints_schema.yml",
            )

            run_dbt(["run", "-s", "my_model_data_type"])

            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config.enforced is True


class BaseTableConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class BaseViewConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class BaseIncrementalConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


class TestTableConstraintsColumnsEqual(BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqual(BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqual(BaseIncrementalConstraintsColumnsEqual):
    pass


class BaseConstraintsRuntimeDdlEnforcement:
    """These constraints pass muster for dbt's preflight checks. Make sure they're
    passed into the DDL statement. If they don't match up with the underlying data,
    the data platform should raise an error at runtime.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
EXEC('create view <model_identifier> as -- depends_on: <foreign_key_model_identifier> select ''blue'' as color, 1 as id, ''2019-01-01'' as date_day;'); CREATE TABLE <model_identifier> ( id int not null, color varchar(100), date_day varchar(100) ) EXEC(' alter table <model_identifier> add constraint <model_identifier> primary key nonclustered(id) not enforced; ;') EXEC(' alter table <model_identifier> add constraint <model_identifier> foreign key(id) references <foreign_key_model_identifier> (id) not enforced; ;') EXEC(' alter table <model_identifier> add constraint <model_identifier> unique nonclustered(id) not enforced; ;') INSERT INTO <model_identifier> ( [id], [color], [date_day] ) SELECT [id], [color], [date_day] FROM <model_identifier> EXEC('DROP view IF EXISTS <model_identifier>
"""

    def test__constraints_ddl(self, project, expected_sql):
        unformatted_constraint_schema_yml = read_file(
            "models", "constraints_schema.yml"
        )
        write_file(
            unformatted_constraint_schema_yml.format(schema=project.test_schema),
            "models",
            "constraints_schema.yml",
        )

        results = run_dbt(["run", "-s", "+my_model"])
        assert len(results) >= 1

        # TODO: consider refactoring this to introspect logs instead
        generated_sql = read_file("target", "run", "test", "models", "my_model.sql")
        generated_sql_generic = _find_and_replace(
            generated_sql, "my_model", "<model_identifier>"
        )
        generated_sql_generic = _find_and_replace(
            generated_sql_generic,
            "foreign_key_model",
            "<foreign_key_model_identifier>",
        )
        generated_sql_wodb = generated_sql_generic.replace(
            "USE [" + project.database + "];", ""
        )
        assert _normalize_whitespace(expected_sql) == _normalize_whitespace(
            generated_sql_wodb
        )


class TestTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    pass


class BaseIncrementalConstraintsRuntimeDdlEnforcement(
    BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }


class TestIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
):
    pass


class TestIncrementalConstraintsRollbackSQLServer(BaseIncrementalConstraintsRollback):
    pass


class BaseModelConstraintsRuntimeEnforcement:
    """These model-level constraints pass muster for dbt's preflight checks. Make sure they're
    passed into the DDL statement. If they don't match up with the underlying data,
    the data platform should raise an error at runtime.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
EXEC('create view <model_identifier> as -- depends_on: <foreign_key_model_identifier> select ''blue'' as color, 1 as id, ''2019-01-01'' as date_day;'); CREATE TABLE <model_identifier> ( id int not null, color varchar(100), date_day varchar(100) ) EXEC(' alter table <model_identifier> add constraint <model_identifier> primary key nonclustered(id) not enforced; ;') EXEC(' alter table <model_identifier> add constraint <model_identifier> unique nonclustered(color, date_day) not enforced; ;') EXEC(' alter table <model_identifier> add constraint <model_identifier> foreign key(id) references <foreign_key_model_identifier> (id) not enforced; ;') INSERT INTO <model_identifier> ( [id], [color], [date_day] ) SELECT [id], [color], [date_day] FROM <model_identifier> EXEC('DROP view IF EXISTS <model_identifier>
"""

    def test__model_constraints_ddl(self, project, expected_sql):
        unformatted_constraint_schema_yml = read_file(
            "models", "constraints_schema.yml"
        )
        write_file(
            unformatted_constraint_schema_yml.format(schema=project.test_schema),
            "models",
            "constraints_schema.yml",
        )

        results = run_dbt(["run", "-s", "+my_model"])
        assert len(results) >= 1
        generated_sql = read_file("target", "run", "test", "models", "my_model.sql")

        generated_sql_generic = _find_and_replace(
            generated_sql, "my_model", "<model_identifier>"
        )
        generated_sql_generic = _find_and_replace(
            generated_sql_generic,
            "foreign_key_model",
            "<foreign_key_model_identifier>",
        )
        generated_sql_wodb = generated_sql_generic.replace(
            "USE [" + project.database + "];", ""
        )
        assert _normalize_whitespace(expected_sql) == _normalize_whitespace(
            generated_sql_wodb
        )


class TestModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    pass


class BaseConstraintsRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_with_nulls_sql

    @pytest.fixture(scope="class")
    def expected_color(self):
        return "blue"

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "Cannot insert the value NULL into column",
            "column does not allow nulls",
            "There is already an object",
        ]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        assert any(msg in error_message for msg in expected_error_messages)

    def test__constraints_enforcement_rollback(
        self,
        project,
        expected_color,
        expected_error_messages,
        null_model_sql,
    ):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1

        # Make a contract-breaking change to the model
        write_file(null_model_sql, "models", "my_model.sql")

        failing_results = run_dbt(["run", "-s", "my_model"], expect_pass=False)
        assert len(failing_results) == 1

        # Verify the previous table still exists
        relation = relation_from_name(project.adapter, "my_model")
        old_model_exists_sql = f"select * from {relation}"
        old_model_exists = project.run_sql(old_model_exists_sql, fetch="all")
        assert len(old_model_exists) == 1
        assert old_model_exists[0][1] == expected_color

        # Confirm this model was contracted
        # TODO: is this step really necessary?
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract
        assert contract_actual_config.enforced is True

        # Its result includes the expected error messages
        self.assert_expected_error_messages(
            failing_results[0].message, expected_error_messages
        )


class BaseIncrementalConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_incremental_with_nulls_sql


class TestTableConstraintsRollback(BaseConstraintsRollback):
    pass


class TestIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    pass
