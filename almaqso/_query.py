import re
import numpy as np
import glob
from astroquery.alma import Alma
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from pyvo.dal.exceptions import DALServiceError


class Query:
    def __init__(self, source_name: str, band: int, cycle: str) -> None:
        """
        Initialize the Query class.

        Args:
            source_name (str): Name of the source to query.
            band (int): Band number to query.
            cycle (str): Cycle number to query.
        """
        self._source_name = source_name
        self._band = band
        self._cycle = cycle
        self._alma = Alma()
        self._alma.archive_url = "https://almascience.nao.ac.jp"

    @retry(
        retry=retry_if_exception_type(DALServiceError),
        stop=stop_after_attempt(3),
        wait=wait_fixed(3),
    )
    def _query(self) -> np.ndarray:
        """
        Query ALMA data using TAP service or myAlma interface.

        Args:
            None

        Returns:
            np.ndarray: Array of unique member OUS UIDs.
        """

        cycle_list = self.parse_selection_string()
        cycle_condition = ""
        if cycle_list:
            proposal_id_years = [cycle+2011 for cycle in cycle_list]
            proposal_id_list = [f"proposal_id LIKE '{proposal_id_year}.%'" for proposal_id_year in proposal_id_years]
            proposal_id_all = " OR ".join(proposal_id_list)
            cycle_condition = f"AND ({proposal_id_all})"
        
        query = f"""
            SELECT *
            FROM ivoa.obscore
            WHERE target_name = '{self._source_name}'
              AND band_list = '{self._band}'
              {cycle_condition}
              AND data_rights = 'Public'
        """

        ret = self._alma.query_tap(query).to_table().to_pandas()
        ret = ret[ret["antenna_arrays"].str.contains("DV|DA")]  # only 12m data
        ret = ret[ret["velocity_resolution"] < 50000]  # only FDM data
        ret = np.unique(ret["member_ous_uid"])

        return ret

    @retry(
        retry=retry_if_exception_type(DALServiceError),
        stop=stop_after_attempt(3),
        wait=wait_fixed(3),
    )
    def _get_data_info(self, uid):
        return self._alma.get_data_info(uid)

    def query(self) -> list[dict]:
        """
        Query ALMA data and get the URLs of the data, the size of the data, and the total size of the data.
        """
        mous_list = self._query()

        files = []

        for mous in mous_list:
            uid_url_table = self._alma.get_data_info(mous)

            if uid_url_table is None:
                continue

            url_size_list = [
                {"url": url, "size_bytes": size}
                for url, size in zip(
                    uid_url_table["access_url"], uid_url_table["content_length"]
                )
                if ".asdm.sdm.tar" in url
            ]

            files.extend(url_size_list)

        return files

    def parse_selection_string(self):
        """
        Parses a CASA-style selection string and returns a sorted list of integers.
    
        Args:
            The selection string (e.g., "0~11;20,24").
    
        Returns:
            A sorted list of unique integers.
        """
        if not self._cycle or not isinstance(self._cycle, str):
            return []
    
        selected_indices = set()
    
        # First, replace all semicolons with commas to standardize the delimiter.
        standardized_str = self._cycle.replace(';', ',')
        # Then, split the string by the single, standardized delimiter.
        items = standardized_str.split(',')
    
        for item in items:
            item = item.strip()
            if not item:
                continue
            
            # The rest of the logic is exactly the same
            if '<' in item:
                try:
                    val = int(item.replace('<', ''))
                    selected_indices.update(range(val))
                except ValueError:
                    print(f"Warning: Invalid specification '{item}' was ignored.")
            elif '~' in item:
                try:
                    start_str, end_str = item.split('~')
                    start = int(start_str)
                    end = int(end_str)
                    selected_indices.update(range(start, end + 1))
                except ValueError:
                    print(f"Warning: Invalid range specification '{item}' was ignored.")
            else:
                try:
                    index = int(item)
                    selected_indices.add(index)
                except ValueError:
                    print(f"Warning: Invalid specification '{item}' was ignored.")
    
        return sorted(list(selected_indices))
