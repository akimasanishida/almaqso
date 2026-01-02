import pytest
from almaqso._query import _create_query


class TestCreateQueryExactMatch:
    """Tests for exact query string validation."""

    def test_exact_query_single_source_band_cycle(self):
        """Test exact query structure with single source, band, and cycle."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_multiple_sources(self):
        """Test exact query with multiple sources."""
        source_names = ["NGC1097", "NGC4945"]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097' OR target_name = 'NGC4945')
          AND (band_list = '7')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_multiple_bands(self):
        """Test exact query with multiple bands."""
        source_names = ["NGC1097"]
        bands = [3, 7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '3' OR band_list = '7')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_multiple_cycles(self):
        """Test exact query with multiple cycles."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0, 1]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2011.%' OR proposal_id LIKE '2012.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_no_sources(self):
        """Test exact query with empty sources."""
        source_names = []
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (band_list = '7')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_no_bands(self):
        """Test exact query with empty bands."""
        source_names = ["NGC1097"]
        bands = []
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_no_cycles(self):
        """Test exact query with empty cycles."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = []

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_all_empty(self):
        """Test exact query with all empty lists."""
        source_names = []
        bands = []
        cycles = []

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_complex(self):
        """Test exact query with complex combination."""
        source_names = ["NGC1097", "NGC4945", "M82"]
        bands = [3, 6, 7]
        cycles = [0, 1, 2]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097' OR target_name = 'NGC4945' OR target_name = 'M82')
          AND (band_list = '3' OR band_list = '6' OR band_list = '7')
          AND (proposal_id LIKE '2011.%' OR proposal_id LIKE '2012.%' OR proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_query_sql_injection_safety(self):
        """Test that special SQL characters are properly handled."""
        # Note: This tests current behavior, but actual SQL injection prevention
        # would require proper parameterization or escaping
        source_names = ["NGC1097'; DROP TABLE ivoa.obscore; --"]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        # The function should include the string as-is (wrapped in quotes)
        assert "target_name = 'NGC1097'; DROP TABLE ivoa.obscore; --'" in result
        # This demonstrates the function doesn't currently have SQL injection protection

    def test_exact_query_special_characters_in_source_name(self):
        """Test exact query with special characters in source name."""
        source_names = ["J2000-1748"]
        bands = [4]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'J2000-1748')
          AND (band_list = '4')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_large_cycle_number(self):
        """Test exact query with large cycle number (cycle 10 = year 2021)."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [10]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2021.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_all_alma_bands(self):
        """Test exact query with all ALMA bands (3-10)."""
        source_names = ["NGC1097"]
        bands = [3, 4, 5, 6, 7, 8, 9, 10]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '3' OR band_list = '4' OR band_list = '5' OR band_list = '6' OR band_list = '7' OR band_list = '8' OR band_list = '9' OR band_list = '10')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_empty_target_name(self):
        """Test exact query with empty target name."""
        source_names = [""]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (band_list = '7')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_contain_empty_target_name(self):
        """Test exact query with a list containing an empty target name."""
        source_names = ["NGC1097", ""]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2011.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()