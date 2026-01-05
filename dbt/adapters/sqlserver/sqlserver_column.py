from typing import Any, ClassVar, Dict

from dbt.adapters.base import Column
from dbt_common.exceptions import DbtRuntimeError


class SQLServerColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(8000)",
        "VARCHAR": "VARCHAR(8000)",
        "CHAR": "CHAR(1)",
        "NCHAR": "CHAR(1)",
        "NVARCHAR": "VARCHAR(8000)",
        "TIMESTAMP": "DATETIME2(6)",
        "DATETIME2": "DATETIME2(6)",
        "DATETIME2(6)": "DATETIME2(6)",
        "DATE": "DATE",
        "TIME": "TIME(6)",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",
        "BIT": "BIT",
        "BOOLEAN": "BIT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "MONEY": "DECIMAL",
        "SMALLMONEY": "DECIMAL",
        "UNIQUEIDENTIFIER": "UNIQUEIDENTIFIER",
        "VARBINARY": "VARBINARY(MAX)",
        "BINARY": "BINARY(1)",
    }

    @classmethod
    def string_type(cls, size: int) -> str:
        return f"varchar({size if size > 0 else '8000'})"

    def literal(self, value: Any) -> str:
        return "cast('{}' as {})".format(value, self.data_type)

    @property
    def data_type(self) -> str:
        # Always enforce datetime2 precision
        if self.dtype.lower() == "datetime2":
            return "datetime2(6)"
        if self.is_string():
            return self.string_type(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ["varchar", "char"]

    def is_number(self):
        return any([self.is_integer(), self.is_numeric(), self.is_float()])

    def is_float(self):
        return self.dtype.lower() in ["float", "real"]

    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            # real types
            "smallint",
            "integer",
            "bigint",
            "smallserial",
            "serial",
            "bigserial",
            # aliases
            "int2",
            "int4",
            "int8",
            "serial2",
            "serial4",
            "serial8",
            "int",
        ]

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ["numeric", "decimal", "money", "smallmoney"]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        if self.char_size is None:
            return 8000
        else:
            return int(self.char_size)

    def can_expand_to(self, other_column: "SQLServerColumn") -> bool:
        if not self.is_string() or not other_column.is_string():
            return False
        return other_column.string_size() > self.string_size()

