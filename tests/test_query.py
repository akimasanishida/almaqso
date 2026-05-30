import pytest
import numpy as np
from almaqso._query import _create_query, query


class TestCreateQueryExactMatch:
    """Tests for exact query string validation."""

    def test_exact_query_single_source_band_cycle(self):
        """Test exact query structure with single source, band, and cycle."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_multiple_sources(self):
        """Test exact query with multiple sources."""
        source_names = ["NGC1097", "NGC4945"]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097' OR target_name = 'NGC4945')
          AND (band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_multiple_bands(self):
        """Test exact query with multiple bands."""
        source_names = ["NGC1097"]
        bands = [3, 7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '3' OR band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_multiple_cycles(self):
        """Test exact query with multiple cycles."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0, 1]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2013.%' OR proposal_id LIKE '2014.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_no_sources(self):
        """Test exact query with empty sources."""
        source_names = []
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_no_bands(self):
        """Test exact query with empty bands."""
        source_names = ["NGC1097"]
        bands = []
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_no_cycles(self):
        """Test exact query with empty cycles."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = []

        result = _create_query(source_names, bands, cycles, [], None)

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

        result = _create_query(source_names, bands, cycles, [], None)

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

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097' OR target_name = 'NGC4945' OR target_name = 'M82')
          AND (band_list = '3' OR band_list = '6' OR band_list = '7')
          AND (proposal_id LIKE '2013.%' OR proposal_id LIKE '2014.%' OR proposal_id LIKE '2015.%')
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

        result = _create_query(source_names, bands, cycles, [], None)

        # The function should include the string as-is (wrapped in quotes)
        assert "target_name = 'NGC1097'; DROP TABLE ivoa.obscore; --'" in result
        # This demonstrates the function doesn't currently have SQL injection protection

    def test_exact_query_special_characters_in_source_name(self):
        """Test exact query with special characters in source name."""
        source_names = ["J2000-1748"]
        bands = [4]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'J2000-1748')
          AND (band_list = '4')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_large_cycle_number(self):
        """Test exact query with large cycle number (cycle 10 = year 2021)."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [10]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2023.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_all_alma_bands(self):
        """Test exact query with all ALMA bands (3-10)."""
        source_names = ["NGC1097"]
        bands = [3, 4, 5, 6, 7, 8, 9, 10]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '3' OR band_list = '4' OR band_list = '5' OR band_list = '6' OR band_list = '7' OR band_list = '8' OR band_list = '9' OR band_list = '10')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_empty_target_name(self):
        """Test exact query with empty target name."""
        source_names = [""]
        bands = [7]
        cycles = [0]

        result = _create_query(source_names, bands, cycles, [], None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_with_project_code(self):
        """Test exact query with project code instead of cycles."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0]  # Should be ignored when project_code is provided
        project_code = ["2013.1.00001.S"]

        result = _create_query(source_names, bands, cycles, project_code, None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id = '2013.1.00001.S')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_with_multiple_project_codes(self):
        """Test exact query with multiple project codes."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = []
        project_code = ["2013.1.00001.S", "2014.1.00002.S"]

        result = _create_query(source_names, bands, cycles, project_code, None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id = '2013.1.00001.S' OR proposal_id = '2014.1.00002.S')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_with_frequency_single(self):
        """Test exact query with single frequency."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0]
        project_code = []
        frequency_ghz = 345.5

        result = _create_query(source_names, bands, cycles, project_code, frequency_ghz)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND ((frequency - 0.5 * bandwidth/1e9) < 345.5 AND (frequency + 0.5 * bandwidth/1e9) > 345.5)
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_with_frequency_range(self):
        """Test exact query with frequency range."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0]
        project_code = []
        frequency_ghz = (340.0, 350.0)

        result = _create_query(source_names, bands, cycles, project_code, frequency_ghz)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id LIKE '2013.%')
          AND ((frequency - 0.5 * bandwidth/1e9) < 350.0 AND (frequency + 0.5 * bandwidth/1e9) > 340.0)
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_project_code_overrides_cycles(self):
        """Test that project_code takes precedence over cycles."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = [0, 1, 2]  # These should be ignored
        project_code = ["2013.1.00001.S"]

        result = _create_query(source_names, bands, cycles, project_code, None)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097')
          AND (band_list = '7')
          AND (proposal_id = '2013.1.00001.S')
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_with_all_parameters(self):
        """Test exact query with all parameters specified."""
        source_names = ["NGC1097", "NGC4945"]
        bands = [3, 7]
        cycles = []  # Ignored because project_code is provided
        project_code = ["2013.1.00001.S", "2014.1.00002.S"]
        frequency_ghz = (100.0, 500.0)

        result = _create_query(source_names, bands, cycles, project_code, frequency_ghz)

        expected = """
        SELECT *
        FROM ivoa.obscore
        WHERE (target_name = 'NGC1097' OR target_name = 'NGC4945')
          AND (band_list = '3' OR band_list = '7')
          AND (proposal_id = '2013.1.00001.S' OR proposal_id = '2014.1.00002.S')
          AND ((frequency - 0.5 * bandwidth/1e9) < 500.0 AND (frequency + 0.5 * bandwidth/1e9) > 100.0)
          AND data_rights = 'Public'
    """

        assert result.strip() == expected.strip()

    def test_exact_query_with_maximum_velocity_resolution_mid(self):
        """Test exact query with maximum velocity resolution specified."""
        source_names = ["NGC1097"]
        bands = [7]
        cycles = []  # Ignored because project_code is provided
        maximum_velocity_resolution = 50.0  # km/s

        result = query(source_names, bands, cycles, [], None, maximum_velocity_resolution)

        expected = [{'url': 'https://almascience.nao.ac.jp/dataPortal/2015.1.00126.S_uid___A002_Xb80c59_X9223.asdm.sdm.tar', 'size_bytes': np.int64(16377745408)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2015.1.00126.S_uid___A002_Xb825df_X609.asdm.sdm.tar', 'size_bytes': np.int64(17589186560)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2015.1.00126.S_uid___A002_Xb825df_X971.asdm.sdm.tar', 'size_bytes': np.int64(31754557440)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2015.1.00126.S_uid___A002_Xb825df_Xc3d.asdm.sdm.tar', 'size_bytes': np.int64(31732332544)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2011.0.00108.S_uid___A002_X30a93d_Xba7.asdm.sdm.tar', 'size_bytes': np.int64(4552451072)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2011.0.00108.S_uid___A002_X30cca6_X342.asdm.sdm.tar', 'size_bytes': np.int64(5194443776)}
                    ]

        assert result == expected

    def test_exact_query_with_maximum_velocity_resolution_low(self):
        """Test exact query with low maximum velocity resolution specified."""
        source_names = ["NGC4945"]
        bands = [6]
        cycles = []  # Ignored because project_code is provided
        maximum_velocity_resolution = 100.0  # km/s

        result = query(source_names, bands, cycles, [], None, maximum_velocity_resolution)

        expected = [{'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X98124f_X2127.asdm.sdm.tar', 'size_bytes': np.int64(3665391616)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2021.1.00783.S_uid___A002_Xf89be2_X4027.asdm.sdm.tar', 'size_bytes': np.int64(34229103616)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2021.1.00783.S_uid___A002_Xf8d822_X4fb9.asdm.sdm.tar', 'size_bytes': np.int64(35797571584)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2021.1.00783.S_uid___A002_Xf8d822_X579e.asdm.sdm.tar', 'size_bytes': np.int64(35797147648)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2021.1.00783.S_uid___A002_Xf8f6a9_X16dfc.asdm.sdm.tar', 'size_bytes': np.int64(39039451136)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2021.1.00783.S_uid___A002_Xf8f6a9_X5e59.asdm.sdm.tar', 'size_bytes': np.int64(34229502976)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X9fa4e2_X6a9f.asdm.sdm.tar', 'size_bytes': np.int64(91015168)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X9fa4e2_X6ce6.asdm.sdm.tar', 'size_bytes': np.int64(90475520)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X9fa4e2_X6fb1.asdm.sdm.tar', 'size_bytes': np.int64(91072512)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X9fa4e2_X71d9.asdm.sdm.tar', 'size_bytes': np.int64(91046912)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X9fa4e2_X75cb.asdm.sdm.tar', 'size_bytes': np.int64(90792960)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2013.1.00099.S_uid___A002_X9fa4e2_X79dd.asdm.sdm.tar', 'size_bytes': np.int64(90382336)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2016.1.01279.S_uid___A002_Xbb44e1_X1d53.asdm.sdm.tar', 'size_bytes': np.int64(6406522880)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2016.1.01279.S_uid___A002_Xbb44e1_X23b1.asdm.sdm.tar', 'size_bytes': np.int64(6409761792)}
                    ]

        assert result == expected

    def test_exact_query_with_maximum_velocity_resolution_high(self):
        """Test exact query with high maximum velocity resolution specified."""
        source_names = ["NGC4945"]
        bands = [6]
        cycles = []  # Ignored because project_code is provided
        maximum_velocity_resolution = 1.0  # km/s

        result = query(source_names, bands, cycles, [], None, maximum_velocity_resolution)

        expected = []

        assert result == expected

    def test_exact_query_with_j062307_200kms(self):
        """Test exact query with high maximum velocity resolution specified."""
        source_names = ["J062307-643620"]
        bands = []
        cycles = []  # Ignored because project_code is provided
        maximum_velocity_resolution = 200.0  # km/s

        result = query(source_names, bands, cycles, [], None, maximum_velocity_resolution)

        expected = [{'url': 'https://almascience.nao.ac.jp/dataPortal/2015.1.01522.S_uid___A002_Xb7189f_Xa9cb.asdm.sdm.tar', 'size_bytes': np.int64(12194554880)},
                    {'url': 'https://almascience.nao.ac.jp/dataPortal/2015.1.01522.S_uid___A002_Xb7189f_Xaf15.asdm.sdm.tar', 'size_bytes': np.int64(10028267520)}
                    ]

        assert result == expected

    def test_exact_query_with_j062307_50kms(self):
        """Test exact query with low maximum velocity resolution specified."""
        source_names = ["J062307-643620"]
        bands = []
        cycles = []  # Ignored because project_code is provided
        maximum_velocity_resolution = 50.0  # km/s

        result = query(source_names, bands, cycles, [], None, maximum_velocity_resolution)

        expected = []

        assert result == expected

        