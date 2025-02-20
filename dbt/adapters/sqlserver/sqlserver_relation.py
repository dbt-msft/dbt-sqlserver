from dataclasses import dataclass, field
from typing import Optional, Type

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter
from dbt.adapters.utils import classproperty
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.sqlserver.relation_configs import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    SQLServerIncludePolicy,
    SQLServerQuotePolicy,
    SQLServerRelationType,
)


@dataclass(frozen=True, eq=False, repr=False)
class SQLServerRelation(BaseRelation):
    type: Optional[SQLServerRelationType] = None  # type: ignore
    include_policy: SQLServerIncludePolicy = field(
        default_factory=lambda: SQLServerIncludePolicy()
    )
    quote_policy: SQLServerQuotePolicy = field(default_factory=lambda: SQLServerQuotePolicy())

    @classproperty
    def get_relation_type(cls) -> Type[SQLServerRelationType]:
        return SQLServerRelationType

    def render_limited(self) -> str:
        rendered = self.render()
        if self.limit is None:
            return rendered
        elif self.limit == 0:
            return f"(select * from {rendered} where 1=0) {self._render_limited_alias()}"
        else:
            return f"(select TOP {self.limit} * from {rendered}) {self._render_limited_alias()}"

    def __post_init__(self):
        # Check for length of Redshift table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > MAX_CHARACTERS_IN_IDENTIFIER
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {MAX_CHARACTERS_IN_IDENTIFIER} characters"
            )

    def relation_max_name_length(self):
        return MAX_CHARACTERS_IN_IDENTIFIER

    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        """
        Returns "" if start and end are both None
        """
        filter = ""
        if event_time_filter.start and event_time_filter.end:
            filter = (
                f"{event_time_filter.field_name} >="
                f" cast('{event_time_filter.start}' as datetimeoffset)"
                f" and {event_time_filter.field_name} <"
                f" cast('{event_time_filter.end}' as datetimeoffset)"
            )
        elif event_time_filter.start:
            filter = (
                f"{event_time_filter.field_name} >="
                f" cast('{event_time_filter.start}' as datetimeoffset)"
            )
        elif event_time_filter.end:
            filter = (
                f"{event_time_filter.field_name} <"
                f" cast('{event_time_filter.end}' as datetimeoffset)"
            )

        return filter
