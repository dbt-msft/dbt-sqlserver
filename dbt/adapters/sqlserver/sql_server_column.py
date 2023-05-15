from typing import Any, ClassVar, Dict

from dbt.adapters.base import Column


class SQLServerColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(MAX)",
        "TIMESTAMP": "DATETIMEOFFSET",
        "FLOAT": "FLOAT",
        "INTEGER": "INT",
        "BOOLEAN": "BIT",
    }

    @classmethod
    def string_type(cls, size: int) -> str:
        return f"varchar({size if size > 0 else 'MAX'})"

    def literal(self, value: Any) -> str:
        return "cast('{}' as {})".format(value, self.data_type)
