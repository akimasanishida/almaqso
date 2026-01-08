import pytest
from almaqso._utils import parse_selection_string, parse_selection, in_source_list, parse_source_list


class TestParseSelectionString:
    """Tests for parse_selection_string function."""

    def test_empty_string(self):
        """Test with empty string."""
        assert parse_selection_string("") == []

    def test_single_number(self):
        """Test with a single number."""
        assert parse_selection_string("5") == [5]

    def test_comma_separated_numbers(self):
        """Test with comma-separated numbers."""
        assert parse_selection_string("1,3,5") == [1, 3, 5]

    def test_semicolon_separated_numbers(self):
        """Test with semicolon-separated numbers."""
        assert parse_selection_string("1;3;5") == [1, 3, 5]

    def test_mixed_separators(self):
        """Test with mixed comma and semicolon separators."""
        assert parse_selection_string("1,3;5") == [1, 3, 5]

    def test_range_with_tilde(self):
        """Test range specification with tilde."""
        assert parse_selection_string("0~5") == [0, 1, 2, 3, 4, 5]

    def test_multiple_ranges(self):
        """Test multiple ranges."""
        assert parse_selection_string("0~2,5~7") == [0, 1, 2, 5, 6, 7]

    def test_less_than_operator(self):
        """Test less than operator."""
        assert parse_selection_string("<5") == [0, 1, 2, 3, 4]

    def test_complex_selection(self):
        """Test complex CASA-style selection string."""
        assert parse_selection_string("0~11;20,24") == [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            20,
            24,
        ]

    def test_overlapping_ranges(self):
        """Test overlapping ranges (should return unique sorted values)."""
        assert parse_selection_string("0~5,3~7") == [0, 1, 2, 3, 4, 5, 6, 7]

    def test_unordered_input(self):
        """Test that output is sorted even with unordered input."""
        assert parse_selection_string("10,5,1,3") == [1, 3, 5, 10]

    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        assert parse_selection_string(" 1 , 3 , 5 ") == [1, 3, 5]

    def test_empty_items(self):
        """Test with empty items in the list."""
        assert parse_selection_string("1,,3,5") == [1, 3, 5]

    def test_invalid_specification_single(self):
        """Test with invalid specification (non-numeric)."""
        with pytest.raises(ValueError, match="Invalid specification"):
            parse_selection_string("abc")

    def test_invalid_range_specification(self):
        """Test with invalid range specification."""
        with pytest.raises(ValueError, match="Invalid range specification"):
            parse_selection_string("1~abc")

    def test_invalid_less_than_specification(self):
        """Test with invalid less than specification."""
        with pytest.raises(ValueError, match="Invalid specification"):
            parse_selection_string("<abc")

    def test_range_reverse_order(self):
        """Test range with start > end."""
        result = parse_selection_string("5~2")
        assert result == []

    def test_negative_numbers(self):
        """Test with negative numbers."""
        assert parse_selection_string("-5,-3,-1") == [-5, -3, -1]

    def test_negative_range(self):
        """Test with negative number range."""
        assert parse_selection_string("-3~0") == [-3, -2, -1, 0]

    def test_large_numbers(self):
        """Test with large numbers."""
        assert parse_selection_string("1000,2000,3000") == [1000, 2000, 3000]

    def test_combination_all_operators(self):
        """Test combination of all operators."""
        assert parse_selection_string("<3,5~7,10,15") == [0, 1, 2, 5, 6, 7, 10, 15]


class TestParseSelection:
    """Tests for parse_selection function."""

    def test_single_integer(self):
        """Test with a single integer."""
        assert parse_selection(5) == [5]

    def test_list_of_integers(self):
        """Test with a list of integers."""
        assert parse_selection([3, 1, 5, 2]) == [1, 2, 3, 5]

    def test_empty_list(self):
        """Test with an empty list."""
        assert parse_selection([]) == []

    def test_string_input(self):
        """Test with string input."""
        assert parse_selection("0~5") == [0, 1, 2, 3, 4, 5]

    def test_string_complex(self):
        """Test with complex string input."""
        assert parse_selection("0~11;20,24") == [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            20,
            24,
        ]

    def test_negative_integer(self):
        """Test with negative integer."""
        assert parse_selection(-5) == [-5]

    def test_list_with_duplicates(self):
        """Test list with duplicate values (should be sorted and unique)."""
        assert parse_selection([3, 1, 3, 2, 1]) == [1, 2, 3]

    def test_zero(self):
        """Test with zero."""
        assert parse_selection(0) == [0]

    def test_list_with_negative_numbers(self):
        """Test list with negative numbers."""
        assert parse_selection([-5, 3, -1, 0]) == [-5, -1, 0, 3]

    def test_invalid_type_float(self):
        """Test with invalid type (float)."""
        with pytest.raises(ValueError, match="Invalid selection input type"):
            parse_selection(5.5)  # pyright: ignore[reportArgumentType]

    def test_invalid_type_dict(self):
        """Test with invalid type (dict)."""
        with pytest.raises(ValueError, match="Invalid selection input type"):
            parse_selection({"key": "value"})  # pyright: ignore[reportArgumentType]

    def test_invalid_type_none(self):
        """Test with None."""
        with pytest.raises(ValueError, match="Invalid selection input type"):
            parse_selection(None)  # pyright: ignore[reportArgumentType]

    def test_string_with_invalid_content(self):
        """Test string with invalid content."""
        with pytest.raises(ValueError):
            parse_selection("invalid")

    def test_large_list(self):
        """Test with large list."""
        large_list = list(range(100, 0, -1))
        result = parse_selection(large_list)
        assert result == list(range(1, 101))

    def test_list_single_element(self):
        """Test list with single element."""
        assert parse_selection([42]) == [42]


class TestInSourceList:
    """Tests for in_source_list function."""

    def test_empty_list_returns_true(self):
        """Test that empty source list returns True for any source."""
        assert in_source_list("J2000-1748", []) is True
        assert in_source_list("AnySource", []) is True
        assert in_source_list("", []) is True

    def test_exact_match(self):
        """Test exact match (same case)."""
        source_list = ["J2000-1748", "3C273", "NGC1068"]
        assert in_source_list("J2000-1748", source_list) is True
        assert in_source_list("3C273", source_list) is True
        assert in_source_list("NGC1068", source_list) is True

    def test_case_insensitive_match(self):
        """Test case-insensitive matching."""
        source_list = ["J2000-1748", "3C273", "NGC1068"]
        assert in_source_list("j2000-1748", source_list) is True
        assert in_source_list("J2000-1748", source_list) is True
        assert in_source_list("3c273", source_list) is True
        assert in_source_list("3C273", source_list) is True
        assert in_source_list("ngc1068", source_list) is True
        assert in_source_list("NGC1068", source_list) is True

    def test_mixed_case_in_list(self):
        """Test case-insensitive matching with mixed case in source list."""
        source_list = ["j2000-1748", "3c273", "NgC1068"]
        assert in_source_list("J2000-1748", source_list) is True
        assert in_source_list("3C273", source_list) is True
        assert in_source_list("NGC1068", source_list) is True
        assert in_source_list("ngc1068", source_list) is True

    def test_not_in_list(self):
        """Test source name not in list."""
        source_list = ["J2000-1748", "3C273", "NGC1068"]
        assert in_source_list("UnknownSource", source_list) is False
        assert in_source_list("J2000-1749", source_list) is False
        assert in_source_list("3C274", source_list) is False

    def test_single_element_list(self):
        """Test with single element in list."""
        source_list = ["J2000-1748"]
        assert in_source_list("J2000-1748", source_list) is True
        assert in_source_list("j2000-1748", source_list) is True
        assert in_source_list("OtherSource", source_list) is False

    def test_partial_match_not_found(self):
        """Test that partial matches are not found."""
        source_list = ["J2000-1748", "3C273"]
        assert in_source_list("J2000", source_list) is False
        assert in_source_list("1748", source_list) is False
        assert in_source_list("3C", source_list) is False

    def test_empty_string_source_name(self):
        """Test with empty string as source name."""
        source_list = ["J2000-1748", "3C273"]
        assert in_source_list("", source_list) is False

    def test_whitespace_in_names(self):
        """Test with whitespace in source names."""
        source_list = ["Source A", "Source B"]
        assert in_source_list("Source A", source_list) is True
        assert in_source_list("source a", source_list) is True
        assert in_source_list("SOURCE A", source_list) is True
        assert in_source_list("Source C", source_list) is False


class TestParseSourceList:
    """Tests for parse_source_list function."""

    def test_list_of_strings(self):
        """Test with a simple list of strings."""
        result = parse_source_list(["J2000-1748", "3C273", "NGC1068"])
        assert sorted(result) == sorted(["3C273", "J2000-1748", "NGC1068"])

    def test_list_with_duplicates(self):
        """Test that duplicates are removed."""
        result = parse_source_list(["J2000-1748", "3C273", "J2000-1748", "NGC1068"])
        assert sorted(result) == sorted(["3C273", "J2000-1748", "NGC1068"])
        assert len(result) == 3

    def test_empty_list(self):
        """Test with empty list."""
        result = parse_source_list([])
        assert result == []

    def test_list_with_empty_strings(self):
        """Test that empty strings are removed."""
        result = parse_source_list(["J2000-1748", "", "3C273", ""])
        assert sorted(result) == sorted(["3C273", "J2000-1748"])
        assert "" not in result

    def test_single_element_list(self):
        """Test with single element list."""
        result = parse_source_list(["J2000-1748"])
        assert result == ["J2000-1748"]

    def test_list_preserves_case(self):
        """Test that case is preserved in list input."""
        result = parse_source_list(["J2000-1748", "j2000-1748"])
        # Both should be present as they are different strings
        assert len(result) == 2
        assert "J2000-1748" in result
        assert "j2000-1748" in result

    def test_list_with_whitespace(self):
        """Test list with strings containing whitespace."""
        result = parse_source_list(["Source A", "Source B", "Source C"])
        assert sorted(result) == sorted(["Source A", "Source B", "Source C"])

    def test_single_string_input(self):
        """Test with single string input (not comma-separated)."""
        result = parse_source_list("J2000-1748")
        # np.unique on a string treats it as array of characters
        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)

    def test_list_only_empty_strings(self):
        """Test with list containing only empty strings."""
        result = parse_source_list(["", "", ""])
        assert result == []

    def test_list_mixed_duplicates_and_empty(self):
        """Test with mix of duplicates and empty strings."""
        result = parse_source_list(["3C273", "", "J2000-1748", "3C273", "", "NGC1068"])
        assert sorted(result) == sorted(["3C273", "J2000-1748", "NGC1068"])
        assert "" not in result
        assert len(result) == 3

    def test_list_with_numeric_strings(self):
        """Test with numeric strings."""
        result = parse_source_list(["123", "456", "123"])
        assert sorted(result) == ["123", "456"]

    def test_list_with_special_characters(self):
        """Test with special characters in source names."""
        result = parse_source_list(["J2000+1748", "3C273", "NGC-1068"])
        assert sorted(result) == sorted(["3C273", "J2000+1748", "NGC-1068"])

    def test_large_list(self):
        """Test with large list."""
        sources = [f"Source{i}" for i in range(100)]
        result = parse_source_list(sources)
        assert len(result) == 100
        assert all(f"Source{i}" in result for i in range(100))

    def test_list_sorted_output(self):
        """Test that output is sorted alphabetically."""
        result = parse_source_list(["Zebra", "Apple", "Mango", "Banana"])
        # np.unique sorts the output
        assert result == sorted(["Apple", "Banana", "Mango", "Zebra"])
