from dataclasses import dataclass

from dbt.adapters.contracts.relation import Policy
from dbt_common.dataclass_schema import StrEnum

MAX_CHARACTERS_IN_IDENTIFIER = 127


class SQLServerRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"


class SQLServerIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class SQLServerQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
