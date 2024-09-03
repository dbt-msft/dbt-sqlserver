from typing import Optional

import dbt.exceptions
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.fabric import FabricAdapter
from dbt.contracts.graph.nodes import ConstraintType

from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation


class SQLServerAdapter(FabricAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = SQLServerConnectionManager
    Column = SQLServerColumn
    Relation = SQLServerRelation

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    @classmethod
    def render_model_constraint(cls, constraint) -> Optional[str]:
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
        elif constraint.type == ConstraintType.check and constraint.expression:
            return f"{constraint_prefix} {constraint.name} check ({constraint.expression})"
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix} {constraint.name} {constraint.expression}"
        else:
            return None

    @classmethod
    def date_function(cls):
        return "getdate()"
