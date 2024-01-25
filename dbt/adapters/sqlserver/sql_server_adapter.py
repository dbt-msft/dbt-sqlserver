# https://github.com/microsoft/dbt-fabric/blob/main/dbt/adapters/fabric/fabric_adapter.py
from dbt.adapters.fabric import FabricAdapter

from dbt.adapters.sqlserver.sql_server_column import SQLServerColumn
from dbt.adapters.sqlserver.sql_server_configs import SQLServerConfigs
from dbt.adapters.sqlserver.sql_server_connection_manager import (
    SQLServerConnectionManager,
)

# from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support


class SQLServerAdapter(FabricAdapter):
    ConnectionManager = SQLServerConnectionManager
    Column = SQLServerColumn
    AdapterSpecificConfigs = SQLServerConfigs

    # _capabilities: CapabilityDict = CapabilityDict(
    #     {
    #         Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
    #         Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
    #     }
    # )

    # region - these are implement in fabric but not in sqlserver
    # _capabilities: CapabilityDict = CapabilityDict(
    #     {
    #         Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
    #         Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
    #     }
    # )
    # CONSTRAINT_SUPPORT = {
    #     ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
    #     ConstraintType.not_null: ConstraintSupport.ENFORCED,
    #     ConstraintType.unique: ConstraintSupport.ENFORCED,
    #     ConstraintType.primary_key: ConstraintSupport.ENFORCED,
    #     ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    # }

    # @available.parse(lambda *a, **k: [])
    # def get_column_schema_from_query(self, sql: str) -> List[BaseColumn]:
    #     """Get a list of the Columns with names and data types from the given sql."""
    #     _, cursor = self.connections.add_select_query(sql)

    #     columns = [
    #         self.Column.create(
    #             column_name, self.connections.data_type_code_to_name(column_type_code)
    #         )
    #         # https://peps.python.org/pep-0249/#description
    #         for column_name, column_type_code, *_ in cursor.description
    #     ]
    #     return columns
    # endregion

    @classmethod
    def date_function(cls):
        return "getdate()"
