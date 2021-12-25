from dataclasses import dataclass
from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sqlserver import SQLServerConnectionManager
from dbt.dataclass_schema import dbtClassMixin, ValidationError
import agate
from typing import (
    Optional, Tuple, Callable, Iterable, Type, Dict, Any, List, Mapping,
    Iterator, Union, Set
)
import dbt.exceptions
import dbt.utils


@dataclass
class SQLServerIndexConfig(dbtClassMixin):
    type: str
    columns: Optional[List[str]] = None
    unique: Optional[bool] = None
    include_columns: Optional[List[str]] = None
    partition_schema: Optional[str] = None
    partition_column: Optional[str] = None
    data_compression: Optional[str] = None

    def render(self, relation):
        """
        Name the index according to the following format:
        index type (cix/ix/ccix/ncix), relation name, key columns (joined by `_`).
        Example index name: cix_customers__customer_id.
        """
        index_types = {
            'clustered': 'cix',
            'nonclustered': 'ix',
            'clustered columnstore': 'ccix',
            'nonclustered columnstore': 'ncix'
        }
        index_type = index_types[self.type.lower()]
        index_name = index_type + '_' + relation.identifier
        if self.columns:
            columns = '_'.join(self.columns)
            index_name += '__' + columns
        return index_name[0:127]

    @classmethod
    def parse(cls, raw_index) -> Optional['SQLServerIndexConfig']:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            ix_config = cls.from_dict(raw_index)
            ix_type = ix_config.type.lower()
            if ix_config.data_compression:
                ix_data_compression = ix_config.data_compression.lower()
            else:
                ix_data_compression = None
            # Check index type
            if ix_type not in ['clustered', 'nonclustered', 'clustered columnstore', 'nonclustered columnstore']:
                dbt.exceptions.raise_compiler_error(
                    f'Invalid index type:\n'
                    f'  Got: {ix_config.type}\n'
                    f'  type should be either: "clustered", "nonclustered", "clustered columnstore", "nonclustered columnstore"'
                )
            # Check columns parameter
            elif ix_type not in ['clustered columnstore'] and not ix_config.columns:
                dbt.exceptions.raise_compiler_error(
                    f'The "columns" parameter is required for all types of indexes (except clustered columnstore).\n'
                    f'  Add the "columns" parameter.'
                )
            # Columns parameter doesn't work with clustered columnstore indexes
            elif ix_type in ['clustered columnstore'] and ix_config.columns:
                dbt.exceptions.raise_compiler_error(
                    f'Clustered columnstore index already contains all columns.\n'
                    f'  Remove the "columns" parameter.'
                )
            # Check unique parameter
            elif ix_config.unique and ix_type not in ['clustered', 'nonclustered']:
                dbt.exceptions.raise_compiler_error(
                    f'Uniqueness does not work with columnstore indexes.\n'
                    f'  Remove the "unique" parameter.'
                )
            # Check include columns parameter
            elif ix_config.include_columns and ix_type not in ['nonclustered']:
                dbt.exceptions.raise_compiler_error(
                    f'Only nonclustered indexes may contain included columns.\n'
                    f'  Remove the "include_columns" parameter.'
                )
            # Check partitioning parameters
            elif ((ix_config.partition_schema and not ix_config.partition_column) or 
                (ix_config.partition_column and not ix_config.partition_schema)):
                dbt.exceptions.raise_compiler_error(
                    f'For partitioning must specify both "partition_schema" and "partition_column" parameters'
                )
            # Check data compression parameter
            elif (ix_config.data_compression
                and ix_data_compression not in ['row', 'page', 'columnstore', 'columnstore_archive']):
                dbt.exceptions.raise_compiler_error(
                    f'Invalid data compression:\n'
                    f'  Got: {ix_config.data_compression}\n'
                    f'  data compression should be either: "row", "page", "columnstore", "columnstore_archive"'
                )
            # Data compression for row-store indexes
            elif (ix_data_compression in ['row', 'page']
                and ix_type not in ['clustered', 'nonclustered']):
                dbt.exceptions.raise_compiler_error(
                    f'ROW and PAGE data compression works only with row-store indexes.\n'
                    f'  Remove or fix the "data_compression" parameter.'
                )
            # Data compression for columnstore indexes
            elif (ix_data_compression in ['columnstore', 'columnstore_archive']
                and ix_type not in ['clustered columnstore', 'nonclustered columnstore']):
                dbt.exceptions.raise_compiler_error(
                    f'COLUMNSTORE and COLUMNSTORE_ARCHIVE data compression works only with columnstore indexes.\n'
                    f'  Remove or fix the "data_compression" parameter.'
                )
            else:
                return cls.from_dict(raw_index)
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            dbt.exceptions.raise_compiler_error(
                f'Could not parse index config: {msg}'
            )
        except TypeError:
            dbt.exceptions.raise_compiler_error(
                f'Invalid index config:\n'
                f'  Got: {raw_index}\n'
                f'  Expected a dictionary with at minimum a "type" key'
            )

@dataclass
class SQLServerConfig(AdapterConfig):
    indexes: Optional[List[SQLServerIndexConfig]] = None

class SQLServerAdapter(SQLAdapter):
    ConnectionManager = SQLServerConnectionManager

    @classmethod
    def date_function(cls):
        return "getdate()"

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        # see https://github.com/fishtown-analytics/dbt/pull/2255
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        length = max_len if max_len > 16 else 16
        return "varchar({})".format(length)

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bit"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "datetime"

    @available
    def parse_index(self, raw_index: Any) -> Optional[SQLServerIndexConfig]:
        return SQLServerIndexConfig.parse(raw_index)

    # Methods used in adapter tests
    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = "hour"
    ) -> str:
        # note: 'interval' is not supported for T-SQL
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"DATEADD({interval},{number},{add_to})"

    def string_add_sql(
        self, add_to: str, value: str, location='append',
    ) -> str:
        """
        `+` is T-SQL's string concatenation operator
        """
        if location == 'append':
            return f"{add_to} + '{value}'"
        elif location == 'prepend':
            return f"'{value}' + {add_to}"
        else:
            raise RuntimeException(
                f'Got an unexpected location value of "{location}"'
            )

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:

        """
        note: using is not supported on Synapse so COLUMNS_EQUAL_SQL is adjsuted
        Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql


COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count on row_count_diff.id = diff_count.id
""".strip()
