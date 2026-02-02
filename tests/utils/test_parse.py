import pytest

from aiomegfile.utils.parse import fullname, parse_boolean, parse_quantity


class TestParseQuantity:
    """Test parse_quantity function."""

    def test_integer_input(self):
        """Test that integer input is returned as-is."""
        assert parse_quantity(100) == 100
        assert parse_quantity(0) == 0
        assert parse_quantity(-5) == -5

    def test_string_without_suffix(self):
        """Test string input without suffix."""
        assert parse_quantity("100") == 100
        assert parse_quantity("0") == 0
        assert parse_quantity("-5") == -5

    def test_base1024_ki_suffix(self):
        """Test Ki (kibibyte) suffix."""
        assert parse_quantity("1Ki") == 1024
        assert parse_quantity("2Ki") == 2048
        assert parse_quantity("10Ki") == 10 * 1024

    def test_base1024_mi_suffix(self):
        """Test Mi (mebibyte) suffix."""
        assert parse_quantity("1Mi") == 1024**2
        assert parse_quantity("8Mi") == 8 * 1024**2

    def test_base1024_gi_suffix(self):
        """Test Gi (gibibyte) suffix."""
        assert parse_quantity("1Gi") == 1024**3
        assert parse_quantity("2Gi") == 2 * 1024**3

    def test_base1024_ti_suffix(self):
        """Test Ti (tebibyte) suffix."""
        assert parse_quantity("1Ti") == 1024**4

    def test_base1024_pi_suffix(self):
        """Test Pi (pebibyte) suffix."""
        assert parse_quantity("1Pi") == 1024**5

    def test_base1024_ei_suffix(self):
        """Test Ei (exbibyte) suffix."""
        assert parse_quantity("1Ei") == 1024**6

    def test_base1000_k_suffix(self):
        """Test k (kilo) suffix."""
        assert parse_quantity("1k") == 1000
        assert parse_quantity("5k") == 5000

    def test_base1000_K_suffix(self):
        """Test K (kilo) suffix - uppercase."""
        assert parse_quantity("1K") == 1000
        assert parse_quantity("10K") == 10000

    def test_base1000_m_suffix(self):
        """Test M (mega) suffix."""
        assert parse_quantity("1M") == 1000**2
        assert parse_quantity("8M") == 8 * 1000**2

    def test_base1000_g_suffix(self):
        """Test G (giga) suffix."""
        assert parse_quantity("1G") == 1000**3

    def test_base1000_t_suffix(self):
        """Test T (tera) suffix."""
        assert parse_quantity("1T") == 1000**4

    def test_base1000_p_suffix(self):
        """Test P (peta) suffix."""
        assert parse_quantity("1P") == 1000**5

    def test_base1000_e_suffix(self):
        """Test E (exa) suffix."""
        assert parse_quantity("1E") == 1000**6

    def test_invalid_number_format(self):
        """Test that invalid number format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid number format"):
            parse_quantity("abc")
        with pytest.raises(ValueError, match="Invalid number format"):
            parse_quantity("1.5Ki")

    def test_invalid_ki_lowercase(self):
        """Test that 'ki' (lowercase k with i) raises ValueError."""
        with pytest.raises(ValueError, match="has unknown suffix"):
            parse_quantity("1ki")


class TestParseBoolean:
    """Test parse_boolean function."""

    def test_none_returns_default(self):
        """Test that None returns default value."""
        assert parse_boolean(None) is False
        assert parse_boolean(None, default=True) is True
        assert parse_boolean(None, default=False) is False

    def test_true_values(self):
        """Test values that should return True."""
        assert parse_boolean("true") is True
        assert parse_boolean("True") is True
        assert parse_boolean("TRUE") is True
        assert parse_boolean("yes") is True
        assert parse_boolean("Yes") is True
        assert parse_boolean("YES") is True
        assert parse_boolean("1") is True

    def test_false_values(self):
        """Test values that should return False."""
        assert parse_boolean("false") is False
        assert parse_boolean("False") is False
        assert parse_boolean("FALSE") is False
        assert parse_boolean("no") is False
        assert parse_boolean("No") is False
        assert parse_boolean("NO") is False
        assert parse_boolean("0") is False
        assert parse_boolean("") is False
        assert parse_boolean("anything") is False


class TestFullname:
    """Test fullname function."""

    def test_builtin_types(self):
        """Test fullname for builtin types."""
        assert fullname("hello") == "str"
        assert fullname(123) == "int"
        assert fullname(3.14) == "float"
        assert fullname([1, 2, 3]) == "list"
        assert fullname({"a": 1}) == "dict"
        assert fullname((1, 2)) == "tuple"
        assert fullname({1, 2}) == "set"
        assert fullname(True) == "bool"
        assert fullname(None) == "NoneType"

    def test_standard_library_types(self):
        """Test fullname for standard library types."""
        from collections import OrderedDict
        from io import BytesIO, StringIO

        # Note: BytesIO/StringIO are implemented in _io module
        assert fullname(BytesIO()) == "_io.BytesIO"
        assert fullname(StringIO()) == "_io.StringIO"
        assert fullname(OrderedDict()) == "collections.OrderedDict"

    def test_custom_class(self):
        """Test fullname for custom class."""

        class MyClass:
            pass

        obj = MyClass()
        result = fullname(obj)
        assert "MyClass" in result
        assert "test_parse" in result

    def test_nested_class(self):
        """Test fullname for nested class."""

        class Outer:
            class Inner:
                pass

        obj = Outer.Inner()
        result = fullname(obj)
        assert "Outer.Inner" in result

    def test_exception_types(self):
        """Test fullname for exception types."""
        assert fullname(ValueError("test")) == "ValueError"
        assert fullname(TypeError("test")) == "TypeError"
