from dataclasses import dataclass
from typing import Any

from dbt.adapters.base.column import Column
from dbt.exceptions import RuntimeException


@dataclass
class SQLServerColumn(Column):
    def is_string(self) -> bool:
        return self.dtype.lower() in ['char', 'varchar', 'nchar', 'nvarchar']

    def is_number(self) -> bool:
        return any([self.is_integer(), self.is_numeric(), self.is_float()])

    def is_float(self) -> bool:
        return self.dtype.lower() in ['real', 'float']

    def is_integer(self) -> bool:
        return self.dtype.lower() in ['tinyint', 'smallint', 'int', 'bigint']

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ['numeric', 'decimal']

    def string_size(self) -> int:
        if not self.is_string():
            raise RuntimeException('Called string_size() on non-string field!')
        else:
            return int(self.char_size)

    def sqlserver_string_type(cls, dtype: str, size: int) -> str:
        if size == -1:  # varchar(max) / nvarchar(max)
            return '{}(max)'.format(dtype)
        else:
            return '{}({})'.format(dtype, size)

    @property
    def data_type(self) -> str:
        if self.is_string():
            return self.sqlserver_string_type(self.dtype, self.string_size())
        elif self.is_numeric():
            return SQLServerColumn.numeric_type(
                self.dtype, self.numeric_precision, self.numeric_scale
            )
        else:
            return self.dtype

    @classmethod
    def string_type(cls, size: int) -> str:
        return 'nvarchar({})'.format(size)

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        if precision is None or scale is None:
            return dtype
        else:
            return '{}({},{})'.format(dtype, precision, scale)
