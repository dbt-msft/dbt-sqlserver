import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn


class TestSQLServerColumnIsString:
    def test_varchar_is_string(self):
        col = SQLServerColumn("c", "varchar", char_size=50)
        assert col.is_string() is True

    def test_char_is_string(self):
        col = SQLServerColumn("c", "char", char_size=10)
        assert col.is_string() is True

    def test_nvarchar_is_string(self):
        col = SQLServerColumn("c", "nvarchar", char_size=100)
        assert col.is_string() is True

    def test_nchar_is_string(self):
        col = SQLServerColumn("c", "nchar", char_size=20)
        assert col.is_string() is True

    def test_int_is_not_string(self):
        col = SQLServerColumn("c", "int")
        assert col.is_string() is False

    def test_numeric_is_not_string(self):
        col = SQLServerColumn("c", "numeric")
        assert col.is_string() is False

    def test_varchar_max_is_max_string(self):
        col = SQLServerColumn("c", "varchar", char_size=-1)
        assert col.is_max_string() is True

    def test_nvarchar_max_is_max_string(self):
        col = SQLServerColumn("c", "nvarchar", char_size=-1)
        assert col.is_max_string() is True

    def test_char_is_not_max_string(self):
        col = SQLServerColumn("c", "char", char_size=-1)
        assert col.is_max_string() is False

    def test_nchar_is_not_max_string(self):
        col = SQLServerColumn("c", "nchar", char_size=-1)
        assert col.is_max_string() is False


class TestSQLServerColumnStringTypeInstance:
    def test_varchar_default(self):
        col = SQLServerColumn("c", "varchar")
        result = col.string_type_instance(100)
        assert result == "varchar(100)"

    def test_varchar_max_bounded(self):
        col = SQLServerColumn("c", "varchar")
        result = col.string_type_instance(0)
        assert result == "varchar(8000)"

    def test_nvarchar(self):
        col = SQLServerColumn("c", "nvarchar")
        result = col.string_type_instance(200)
        assert result == "nvarchar(200)"

    def test_nvarchar_max_bounded(self):
        col = SQLServerColumn("c", "nvarchar")
        result = col.string_type_instance(0)
        assert result == "nvarchar(4000)"

    def test_nchar(self):
        col = SQLServerColumn("c", "nchar")
        result = col.string_type_instance(50)
        assert result == "nchar(50)"

    def test_nchar_max_bounded(self):
        col = SQLServerColumn("c", "nchar")
        result = col.string_type_instance(0)
        assert result == "nchar(1)"

    def test_char_default(self):
        col = SQLServerColumn("c", "char")
        result = col.string_type_instance(5)
        assert result == "char(5)"

        result = col.string_type_instance(0)
        assert result == "char(1)"

    def test_varchar_max_emits_varchar_max(self):
        col = SQLServerColumn("c", "varchar")
        result = col.string_type_instance(-1)
        assert result == "varchar(max)"

    def test_nvarchar_max_emits_nvarchar_max(self):
        col = SQLServerColumn("c", "nvarchar")
        result = col.string_type_instance(-1)
        assert result == "nvarchar(max)"

    def test_char_max_raises(self):
        col = SQLServerColumn("c", "char")
        with pytest.raises(DbtRuntimeError, match=r"char\(max\) is not a valid SQL Server type"):
            col.string_type_instance(-1)

    def test_nchar_max_raises(self):
        col = SQLServerColumn("c", "nchar")
        with pytest.raises(DbtRuntimeError, match=r"nchar\(max\) is not a valid SQL Server type"):
            col.string_type_instance(-1)


class TestSQLServerColumnDataType:
    def test_varchar_data_type(self):
        col = SQLServerColumn("c", "varchar", char_size=100)
        assert col.data_type == "varchar(100)"

    def test_nvarchar_data_type(self):
        col = SQLServerColumn("c", "nvarchar", char_size=200)
        assert col.data_type == "nvarchar(200)"


class TestSQLServerColumnIsFixedNumeric:
    def test_money(self):
        col = SQLServerColumn("c", "money")
        assert col.is_fixed_numeric() is True

    def test_smallmoney(self):
        col = SQLServerColumn("c", "smallmoney")
        assert col.is_fixed_numeric() is True

    def test_numeric_is_not_fixed(self):
        col = SQLServerColumn("c", "numeric")
        assert col.is_fixed_numeric() is False


class TestSQLServerColumnIsNumeric:
    def test_numeric(self):
        col = SQLServerColumn("c", "numeric")
        assert col.is_numeric() is True

    def test_decimal(self):
        col = SQLServerColumn("c", "decimal")
        assert col.is_numeric() is True

    def test_money_is_numeric(self):
        col = SQLServerColumn("c", "money")
        assert col.is_numeric() is True

    def test_smallmoney_is_numeric(self):
        col = SQLServerColumn("c", "smallmoney")
        assert col.is_numeric() is True


class TestSQLServerColumnIsDecimalType:
    def test_numeric_is_decimal(self):
        col = SQLServerColumn("c", "numeric")
        assert col.is_decimal_type() is True

    def test_decimal_is_decimal(self):
        col = SQLServerColumn("c", "decimal")
        assert col.is_decimal_type() is True

    def test_money_is_not_decimal(self):
        col = SQLServerColumn("c", "money")
        assert col.is_decimal_type() is False

    def test_smallmoney_is_not_decimal(self):
        col = SQLServerColumn("c", "smallmoney")
        assert col.is_decimal_type() is False


class TestSQLServerColumnStringSize:
    def test_string_size_with_char_size(self):
        col = SQLServerColumn("c", "varchar", char_size=100)
        assert col.string_size() == 100

    def test_string_size_none_char_size(self):
        col = SQLServerColumn("c", "varchar")
        assert col.string_size() == 8000

    def test_string_size_raises_on_non_string(self):
        col = SQLServerColumn("c", "int")
        with pytest.raises(DbtRuntimeError, match="Called string_size"):
            col.string_size()
