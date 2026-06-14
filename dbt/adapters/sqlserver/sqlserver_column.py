from typing import Any, ClassVar, Dict

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.column import Column


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
        """Class-level string_type used by SQLAdapter.expand_column_types.

        Return a VARCHAR default for the SQLAdapter path; this keeps behaviour
        consistent with the rest of dbt where class-level string_type is
        generic and not instance-aware.
        """
        return f"varchar({size if size > 0 else '8000'})"

    def string_type_instance(self, size: int) -> str:
        """Instance-level string type selection that respects NVARCHAR/NCHAR."""
        dtype = (self.dtype or "").lower()
        if dtype == "nvarchar":
            return f"nvarchar({size if size > 0 else '4000'})"
        if dtype == "nchar":
            return f"nchar({size if size > 0 else '1'})"
        if dtype == "char":
            return f"char({size if size > 0 else '1'})"
        return f"varchar({size if size > 0 else '8000'})"

    def literal(self, value: Any) -> str:
        return "cast('{}' as {})".format(value, self.data_type)

    @property
    def data_type(self) -> str:
        # Always enforce datetime2 precision
        if self.dtype.lower() == "datetime2":
            return "datetime2(6)"
        if self.is_string():
            return self.string_type_instance(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ["varchar", "char", "nvarchar", "nchar"]

    def is_number(self):
        return any(
            [self.is_integer(), self.is_numeric(), self.is_float(), self.is_fixed_numeric()]
        )

    def is_float(self):
        return self.dtype.lower() in ["float", "real"]

    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            "smallint",
            "integer",
            "bigint",
            "smallserial",
            "serial",
            "bigserial",
            "int2",
            "int4",
            "int8",
            "serial2",
            "serial4",
            "serial8",
            "int",
            "tinyint",
            "bit",
        ]

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ["numeric", "decimal"]

    def is_fixed_numeric(self) -> bool:
        return self.dtype.lower() in ["money", "smallmoney"]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        if self.char_size is None:
            return 8000
        else:
            return int(self.char_size)

    def can_expand_to(self, other_column: "Column") -> bool:
        self_dtype = self.dtype.lower()
        other_dtype = other_column.dtype.lower()
        if self.is_string() and other_column.is_string():
            self_size = self.string_size()
            other_size = other_column.string_size()
            if other_size > self_size and self_dtype == other_dtype:
                return True
        return False

    def can_expand_safe(self, other_column: "SQLServerColumn") -> bool:
        self_dtype = self.dtype.lower()
        other_dtype = other_column.dtype.lower()

        if self.is_string() and other_column.is_string():
            self_size = self.string_size()
            other_size = other_column.string_size()
            if self_dtype in ("varchar", "char") and other_dtype in ("nvarchar", "nchar"):
                return other_size >= self_size
            return False

        if not self.is_number() or not other_column.is_number():
            return False

        int_family = ("bit", "tinyint", "smallint", "int", "bigint")
        if self_dtype in int_family and other_dtype in int_family:
            return int_family.index(other_dtype) > int_family.index(self_dtype)

        self_prec = int(self.numeric_precision or 0)
        other_prec = int(other_column.numeric_precision or 0)

        if self.is_integer() and other_column.is_numeric():
            minimum_int_precision: int
            if self_dtype in ("tinyint",):
                minimum_int_precision = 3
            elif self_dtype in ("smallint", "int2"):
                minimum_int_precision = 5
            elif self_dtype in ("bigint", "int8", "bigserial", "serial8"):
                minimum_int_precision = 19
            elif self_dtype in ("bit",):
                minimum_int_precision = 1
            else:
                minimum_int_precision = 10
            effective_self_prec = max(self_prec, minimum_int_precision)
            if other_prec >= effective_self_prec:
                return True

        if (self.is_numeric() or self.is_fixed_numeric()) and (
            other_column.is_numeric() or other_column.is_fixed_numeric()
        ):
            self_scale = int(self.numeric_scale or 0)
            other_scale = int(other_column.numeric_scale or 0)

            if other_prec >= self_prec and other_scale >= self_scale:
                if other_prec > self_prec or other_scale > self_scale or self_dtype != other_dtype:
                    return True

        return False


class SQLServerColumnNative(SQLServerColumn):
    """STRING maps to VARCHAR(MAX) (matches dbt convention) and NCHAR / NVARCHAR
    map to their unicode SQL Server types — fixing the legacy default where
    they were silently aliased to non-unicode CHAR(1) / VARCHAR(8000).
    NVARCHAR uses the maximum fixed-length form (4000 — the cap for fixed
    NVARCHAR since unicode is two bytes per character), parallel to VARCHAR(8000).
    Opt-in via the `dbt_sqlserver_use_native_string_types` behaviour flag;
    intended to become the default in a future release."""

    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        **SQLServerColumn.TYPE_LABELS,
        "STRING": "VARCHAR(MAX)",
        "NCHAR": "NCHAR(1)",
        "NVARCHAR": "NVARCHAR(4000)",
    }
