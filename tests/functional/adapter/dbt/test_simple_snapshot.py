from typing import Iterable

from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSimpleSnapshot
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import relation_from_name, run_dbt


def clone_table_sqlserver(
    project: TestProjInfo, to_table: str, from_table: str, select: str, where: str = None
):
    """
    Creates a new table based on another table in a dbt project

    Args:
        project: the dbt project that contains the table
        to_table: the name of the table, without a schema, to be created
        from_table: the name of the table, without a schema, to be cloned
        select: the selection clause to apply on `from_table`; defaults to all columns (*)
        where: the where clause to apply on `from_table`, if any; defaults to all records

    We override this for sqlserver as its using `select into` instead of `create table as select`
    """
    to_table_name = relation_from_name(project.adapter, to_table)
    from_table_name = relation_from_name(project.adapter, from_table)
    select_clause = select or "*"
    where_clause = where or "1 = 1"
    sql = f"drop table if exists {to_table_name}"
    project.run_sql(sql)
    sql = f"""
        select {select_clause}
        into {to_table_name}
        from {from_table_name}
        where {where_clause}
    """
    project.run_sql(sql)


def add_column_sqlserver(project: TestProjInfo, table: str, column: str, definition: str):
    """
    Applies updates to a table in a dbt project

    Args:
        project: the dbt project that contains the table
        table: the name of the table without a schema
        column: the name of the new column
        definition: the definition of the new column, e.g. 'varchar(20) default null'
    """
    # BigQuery doesn't like 'varchar' in the definition
    if project.adapter.type() == "bigquery" and "varchar" in definition.lower():
        definition = "string"
    table_name = relation_from_name(project.adapter, table)
    sql = f"""
        alter table {table_name}
        add {column} {definition}
    """
    project.run_sql(sql)


class TestSimpleSnapshot(BaseSimpleSnapshot):
    def create_fact_from_seed(self, where: str = None):  # type: ignore
        clone_table_sqlserver(self.project, "fact", "seed", "*", where)

    def add_fact_column(self, column: str = None, definition: str = None):  # type: ignore
        add_column_sqlserver(self.project, "fact", column, definition)

    def test_updates_are_captured_by_snapshot(self, project):
        """
        Update the last 5 records. Show that all ids are current, but the last 5 reflect updates.
        """
        self.update_fact_records(
            {"updated_at": "DATEADD(DAY, 1, updated_at)"}, "id between 16 and 20"
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(16, 21),
        )

    def test_new_column_captured_by_snapshot(self, project):
        """
        Add a column to `fact` and populate the last 10 records with a non-null value.
        Show that all ids are current, but the last 10 reflect updates and the first 10 don't
        i.e. if the column is added, but not updated, the record doesn't reflect that it's updated
        """
        self.add_fact_column("full_name", "varchar(200) default null")
        self.update_fact_records(
            {
                "full_name": "first_name + ' ' + last_name",
                "updated_at": "DATEADD(DAY, 1, updated_at)",
            },
            "id between 11 and 20",
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(11, 21),
        )

    def _assert_results(
        self,
        ids_with_current_snapshot_records: Iterable,
        ids_with_closed_out_snapshot_records: Iterable,
    ):
        """
        All test cases are checked by considering whether a
        source record's id has a value in `dbt_valid_to`
        in `snapshot`. Each id can fall into one of the following cases:

        - The id has only one record in `snapshot`; it has a value in `dbt_valid_to`
            - the record was hard deleted in the source
        - The id has only one record in `snapshot`;
          it does not have a value in `dbt_valid_to`
            - the record was not updated in the source
            - the record was updated in the source,
                but not in a way that is tracked (e.g. via `strategy='check'`)
        - The id has two records in `snapshot`;
          one has a value in `dbt_valid_to`, the other does not
            - the record was altered in the source in a way that is tracked
            - the record was hard deleted and revived

        Note: Because of the third scenario, ids may show up in both arguments of this method.

        Args:
            ids_with_current_snapshot_records: a list/set/etc. of ids which aren't end-dated
            ids_with_closed_out_snapshot_records: a list/set/etc. of ids which are end-dated
        """
        records = set(
            self.get_snapshot_records(
                "id, CASE WHEN dbt_valid_to is null then 1 else 0 END as is_current"
            )
        )
        expected_records = set().union(
            {(i, 1) for i in ids_with_current_snapshot_records},
            {(i, 0) for i in ids_with_closed_out_snapshot_records},
        )
        for record in records:
            assert record in expected_records
