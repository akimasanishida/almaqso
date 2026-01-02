import numpy as np
from astroquery.alma import Alma
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from pyvo.dal.exceptions import DALServiceError


def _create_query(source_names: list[str], bands: list[int], cycles: list[int]) -> str:
    """
    Create a TAP query string for the given source names, bands, and cycles.

    Args:
        source_names: List of source names.
        bands: List of band numbers.
        cycles: List of cycle numbers.

    Returns:
        str: The constructed TAP query string.
    """
    conditions = []

    # Remove empty source names "" from the list
    source_names = [name for name in source_names if name.strip()]

    if source_names:
        query_sources = " OR ".join(
            [f"target_name = '{source_name}'" for source_name in source_names]
        )
        conditions.append(f"({query_sources})")

    if bands:
        query_bands = " OR ".join([f"band_list = '{band}'" for band in bands])
        conditions.append(f"({query_bands})")

    if cycles:
        query_cycles = " OR ".join(
            [f"proposal_id LIKE '{cycle + 2011}.%'" for cycle in cycles]
        )
        conditions.append(f"({query_cycles})")

    conditions.append("data_rights = 'Public'")

    where_clause = "\n          AND ".join(conditions)

    query = f"""
        SELECT *
        FROM ivoa.obscore
        WHERE {where_clause}
    """

    return query.strip() + "\n"


@retry(
    retry=retry_if_exception_type(DALServiceError),
    stop=stop_after_attempt(5),
    wait=wait_fixed(3),
)
def query(source_names: list[str], bands: list[int], cycles: list[int]) -> list[dict]:
    """
    Query ALMA data and get the URLs of the data, the size of the data, and the total size of the data.

    12m-array-only and FDM-only data are selected.

    Args:
        source_names: List of source names.
        bands: List of band numbers.
        cycles: List of cycle numbers.

    Returns:
        list[dict]: A list of dictionaries containing 'url' and 'size_bytes' for each data file.
    """
    alma = Alma()
    alma.archive_url = "https://almascience.nao.ac.jp"

    query = _create_query(source_names, bands, cycles)

    mous_list_pd = alma.query_tap(query).to_table().to_pandas()
    mous_list_pd_only_12m = mous_list_pd[
        mous_list_pd["antenna_arrays"].str.contains("DV|DA")
    ]  # only 12m data
    mous_list_pd_only_12m_fdm = mous_list_pd_only_12m[
        mous_list_pd_only_12m["velocity_resolution"] < 50000
    ]  # only FDM data
    mous_list = np.unique(mous_list_pd_only_12m_fdm["member_ous_uid"])

    files = []

    for mous in mous_list:
        uid_url_table = alma.get_data_info(mous)

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
