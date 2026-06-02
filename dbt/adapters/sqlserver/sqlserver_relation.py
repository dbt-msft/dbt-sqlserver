from dataclasses import dataclass, field
from typing import ClassVar, Optional, Type

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter
from dbt.adapters.sqlserver.relation_configs import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    SQLServerIncludePolicy,
    SQLServerQuotePolicy,
    SQLServerRelationType,
)
from dbt.adapters.utils import classproperty


@dataclass(frozen=True, eq=False, repr=False)
class SQLServerRelation(BaseRelation):
    type: Optional[SQLServerRelationType] = None  # type: ignore
    include_policy: SQLServerIncludePolicy = field(
        default_factory=lambda: SQLServerIncludePolicy()
    )
    quote_policy: SQLServerQuotePolicy = field(default_factory=lambda: SQLServerQuotePolicy())
    disable_empty_relation_aliases: ClassVar[bool] = True

    @classproperty
    def get_relation_type(cls) -> Type[SQLServerRelationType]:
        return SQLServerRelationType

    def _render_limited_alias(self) -> str:
        if self.disable_empty_relation_aliases:
            return ""

        return super()._render_limited_alias()

    def render_limited(self) -> str:
        rendered = self.render()
        if self.limit is None:
            return rendered
        elif self.limit == 0:
            return f"(select * from {rendered} where 1=0){self._render_limited_alias()}"
        else:
            return f"(select TOP {self.limit} * from {rendered}){self._render_limited_alias()}"

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
                f" cast('{event_time_filter.start}' as datetime2)"
                f" and {event_time_filter.field_name} <"
                f" cast('{event_time_filter.end}' as datetime2)"
            )
        elif event_time_filter.start:
            filter = (
                f"{event_time_filter.field_name} >="
                f" cast('{event_time_filter.start}' as datetime2)"
            )
        elif event_time_filter.end:
            filter = (
                f"{event_time_filter.field_name} <"
                f" cast('{event_time_filter.end}' as datetime2)"
            )

        return filter
