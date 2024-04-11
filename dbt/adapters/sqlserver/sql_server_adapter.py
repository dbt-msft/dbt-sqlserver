# https://github.com/microsoft/dbt-fabric/blob/main/dbt/adapters/fabric/fabric_adapter.py
from typing import Optional

import dbt.exceptions
from dbt.adapters.fabric import FabricAdapter
from dbt.contracts.graph.nodes import ConstraintType, ModelLevelConstraint

from dbt.adapters.sqlserver.sql_server_column import SQLServerColumn
from dbt.adapters.sqlserver.sql_server_configs import SQLServerConfigs
from dbt.adapters.sqlserver.sql_server_connection_manager import SQLServerConnectionManager

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
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        constraint_prefix = "add constraint "
        column_list = ", ".join(constraint.columns)

        if constraint.name is None:
            raise dbt.exceptions.DbtDatabaseError(
                "Constraint name cannot be empty. Provide constraint name  - column "
                + column_list
                + " and run the project again."
            )

        if constraint.type == ConstraintType.unique:
            return constraint_prefix + f"{constraint.name} unique nonclustered({column_list})"
        elif constraint.type == ConstraintType.primary_key:
            return constraint_prefix + f"{constraint.name} primary key nonclustered({column_list})"
        elif constraint.type == ConstraintType.foreign_key and constraint.expression:
            return (
                constraint_prefix
                + f"{constraint.name} foreign key({column_list}) references "
                + constraint.expression
            )
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix}{constraint.expression}"
        else:
            return None

    @classmethod
    def date_function(cls):
        return "getdate()"
