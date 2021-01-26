from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException


@dataclass
class SQLServerQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SQLServerIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SQLServerRelation(BaseRelation):
    quote_policy: SQLServerQuotePolicy = SQLServerQuotePolicy()
    include_policy: SQLServerIncludePolicy = SQLServerIncludePolicy()
    quote_character: str = '\''