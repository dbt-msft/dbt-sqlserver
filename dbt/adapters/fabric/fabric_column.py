from typing import Any, ClassVar, Dict

from dbt.adapters.base import Column


class FabricColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(8000)",
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
