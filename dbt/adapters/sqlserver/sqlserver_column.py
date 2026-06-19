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
        """Instance-level string type selection that respects NVARCHAR/NCHAR.

        Handles MAX strings (size == -1) by emitting the appropriate
        varchar(max) or nvarchar(max) DDL. Fixed-length char/nchar do not
        support MAX and raise if queried with size == -1.
        """
        dtype = (self.dtype or "").lower()
        if size == -1:
            if dtype == "varchar":
                return "varchar(max)"
            if dtype == "nvarchar":
                return "nvarchar(max)"
            raise DbtRuntimeError(f"{dtype}(max) is not a valid SQL Server type")
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
        elif self.is_decimal_type():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ["varchar", "char", "nvarchar", "nchar"]

    def is_max_string(self) -> bool:
        """Return True if this is a MAX string column (char_size == -1).

        In SQL Server, MAX is represented as -1 in the catalog views.
        This applies to varchar(max) and nvarchar(max). char/nchar do not
        support MAX.
        """
        dtype = (self.dtype or "").lower()
        return dtype in ("varchar", "nvarchar") and int(self.char_size or 0) == -1

    def is_number(self):
        return any([self.is_integer(), self.is_numeric(), self.is_float()])

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
        return self.dtype.lower() in ["numeric", "decimal", "money", "smallmoney"]

    def is_decimal_type(self) -> bool:
        """Return True for true arbitrary-precision numeric/decimal types only.

        This excludes fixed-scale money/smallmoney which are still classified
        as numeric by is_numeric() for backward compatibility.
        """
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

    def can_expand_to(self, other_column: "SQLServerColumn") -> bool:
        self_dtype = self.dtype.lower()
        other_dtype = other_column.dtype.lower()
        if self.is_string() and other_column.is_string():
            if self_dtype != other_dtype:
                return False
            self_max = self.is_max_string()
            other_max = other_column.is_max_string()
            # MAX -> MAX: not an expansion
            if self_max and other_max:
                return False
            # MAX -> bounded: rejected (would be a shrink)
            if self_max and not other_max:
                return False
            # bounded -> MAX: always an expansion
            if not self_max and other_max:
                return True
            # bounded -> bounded: normal numeric size comparison
            return other_column.string_size() > self.string_size()
        return False

    @staticmethod
    def _integer_digits(col: "SQLServerColumn") -> int:
        """Return the number of integer digits for a numeric/integer column.

        For numeric/decimal columns: precision - scale.
        For integer types: the maximum decimal precision required.
        For fixed-money types: precision - scale of their effective representation.
        """
        dtype = col.dtype.lower()
        if col.is_decimal_type():
            prec = int(col.numeric_precision or 0)
            scale = int(col.numeric_scale or 0)
            return prec - scale
        if col.is_fixed_numeric():
            # Treat money/smallmoney as fixed-scale numerics
            if dtype == "smallmoney":
                return 10 - 4  # effectively numeric(10,4)
            elif dtype == "money":
                return 19 - 4  # effectively numeric(19,4)
        if col.is_integer():
            if dtype in ("bit",):
                return 1
            if dtype in ("tinyint",):
                return 3
            if dtype in ("smallint", "int2"):
                return 5
            if dtype in ("bigint", "int8", "bigserial", "serial8"):
                return 19
            # int, integer, int4, serial, serial4, etc.
            return 10
        return 0

    @staticmethod
    def _scale(col: "SQLServerColumn") -> int:
        """Return the scale for numeric / fixed-money columns."""
        if col.is_decimal_type():
            return int(col.numeric_scale or 0)
        if col.is_fixed_numeric():
            # smallmoney and money both have scale 4
            return 4
        return 0

    def can_expand_safe(self, other_column: "SQLServerColumn") -> bool:
        self_dtype = self.dtype.lower()
        other_dtype = other_column.dtype.lower()

        if self.is_string() and other_column.is_string():
            # Cross-family varchar/char -> nvarchar/nchar guarded expansion
            # Also nchar -> nvarchar (fixed-width unicode to variable-width unicode)
            if (self_dtype in ("varchar", "char") and other_dtype in ("nvarchar", "nchar")) or (
                self_dtype == "nchar" and other_dtype == "nvarchar"
            ):
                self_max = self.is_max_string()
                other_max = other_column.is_max_string()

                # varchar(max) -> nvarchar(max): allowed behind safe flag
                if self_max and other_max:
                    return True
                # varchar(max) -> nvarchar(n): rejected for every bounded n
                if self_max and not other_max:
                    return False
                # varchar(n) -> nvarchar(max): allowed
                if not self_max and other_max:
                    return True
                # varchar(n) -> nvarchar(m): normal bounded comparison
                return other_column.string_size() >= self.string_size()

            # Same-family string handled by can_expand_to
            return False

        if not self.is_number() or not other_column.is_number():
            return False

        int_family = ("bit", "tinyint", "smallint", "int", "bigint")
        if self_dtype in int_family and other_dtype in int_family:
            return int_family.index(other_dtype) > int_family.index(self_dtype)

        # Integer -> decimal/numeric expansion
        if self.is_integer() and other_column.is_decimal_type():
            source_int_digits = self._integer_digits(self)
            target_scale = self._scale(other_column)
            target_int_digits = self._integer_digits(other_column)
            return target_scale >= 0 and target_int_digits >= source_int_digits

        # Numeric/decimal <-> fixed-money type expansion
        if (self.is_decimal_type() or self.is_fixed_numeric()) and (
            other_column.is_decimal_type() or other_column.is_fixed_numeric()
        ):
            source_scale = self._scale(self)
            target_scale = self._scale(other_column)
            source_int_digits = self._integer_digits(self)
            target_int_digits = self._integer_digits(other_column)

            if target_scale >= source_scale and target_int_digits >= source_int_digits:
                # Must be a real widening — a pure type rename without
                # increasing integer digits or scale is not an expansion.
                if target_int_digits > source_int_digits or target_scale > source_scale:
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
