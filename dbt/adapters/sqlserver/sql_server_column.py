from typing import ClassVar, Dict

from dbt.adapters.base import Column


class SQLServerColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(MAX)",
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "FLOAT",
        "INTEGER": "INT",
    }
