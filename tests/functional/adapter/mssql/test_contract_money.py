from decimal import Decimal

import pytest

from dbt.tests.util import run_dbt

money_contract_sql = """
{{ config(materialized="table") }}
select cast(12.34 as money) as amount, cast(5.6789 as smallmoney) as fee
"""

money_contract_yml = """
version: 2
models:
  - name: money_contract
    config:
      contract:
        enforced: true
    columns:
      - name: amount
        data_type: money
      - name: fee
        data_type: smallmoney
"""


class TestMoneyContract:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "money_contract.sql": money_contract_sql,
            "money_contract.yml": money_contract_yml,
        }

    def test_money_columns_keep_type_and_cents(self, project):
        run_dbt(["run"])

        types = project.run_sql(
            "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS"
            " where TABLE_SCHEMA = '{schema}' and TABLE_NAME = 'money_contract'",
            fetch="all",
        )
        assert dict(types) == {"amount": "money", "fee": "smallmoney"}

        row = project.run_sql("select amount, fee from {schema}.money_contract", fetch="one")
        assert tuple(row) == (Decimal("12.34"), Decimal("5.6789"))
