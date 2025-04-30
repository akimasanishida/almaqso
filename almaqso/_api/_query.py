import numpy as np
from astroquery.alma import Alma
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from pyvo.dal.exceptions import DALServiceError


class Query:
    def __init__(self, source_name: str, band: int) -> None:
        """
        Initialize the Query class.

        Args:
            source_name (str): Name of the source to query.
            band (int): Band number to query.
        """
        self._source_name = source_name
        self._band = band
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
        query = f"""
            SELECT *
            FROM ivoa.obscore
            WHERE target_name = '{self._source_name}'
              AND band_list = '{self._band}'
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
