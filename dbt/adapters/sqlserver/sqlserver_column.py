from dbt.adapters.fabric import FabricColumn


class SQLServerColumn(FabricColumn):
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

    def is_string(self) -> bool:
        return self.dtype.lower() in [
            "text",
            "character varying",
            "character",
            "varchar",
            "nvarchar",
        ]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")

        if self.dtype == "text" or self.char_size is None:
            # char_size should never be None. Handle it reasonably just in case
            return 256
        elif self.dtype.lower() == "nvarchar":
            # char_size is doubled for nvarchar
            return int(self.char_size // 2)
        else:
            return int(self.char_size)

    def string_type(self, size: int) -> str:
        if self.dtype:
            return f"{self.dtype}({size if size > 0 else '8000'})"
        else:
            return f"varchar({size if size > 0 else '8000'})"
