import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor
from logging import StreamHandler, Formatter, INFO, getLogger
import logging


class _QSOquery:
    """
    Query the ALMA Science Archive for the QSO data.

    Attributes:
        sname (str): Source name.
        band (str): Band number.
        almaurl (str): ALMA Science Archive URL.
        download_d (str): Download directory.
        replaceNAOJ (bool): Replace NAOJ URL.
        only12m (bool): Only 12m data.
        onlyFDM (bool): Only FDM data.
    """

    def __init__(self, sname, band='4',
                 almaurl='https://almascience.nao.ac.jp',
                 download_d='./',
                 replaceNAOJ=False, only12m=False, onlyFDM=False):
        """
        Initialize the class.

        Args:
            sname (str): Source name.
            band (str): Band number.
            almaurl (str): ALMA Science Archive URL. Default is 'https://almascience.nao.ac.jp'.
            download_d (str): Download directory. Default is './'.
            replaceNAOJ (bool): Replace NAOJ URL. Default is False.
            only12m (bool): Only 12m data. Default is False.
            onlyFDM (bool): Only FDM data. Default is False.
        """
        from astroquery.alma import Alma
        self.sname = sname
        self.band = band
        self.almaurl = almaurl
        self.myAlma = Alma()
        self.myAlma.archive_url = almaurl
        self.download_d = download_d
        self.replaceNAOJ = replaceNAOJ
        self.only12m = only12m
        self.onlyFDM = onlyFDM

    def queryALMA(self, almaquery=True):
        """
        Query ALMA data using TAP service or myAlma interface.

        Args:
            almaquery (bool): If True, use self.myAlma for querying. Otherwise, use pyvo TAP service.

        Returns:
            pandas.DataFrame: Filtered query results.
        """
        # SQL query construction
        query = f"""
            SELECT *
            FROM ivoa.obscore
            WHERE target_name = '{self.sname}'
              AND band_list = '{self.band}'
              AND data_rights = 'Public'
        """

        # Perform query using myAlma or pyvo TAP service
        if almaquery:
            ret = self.myAlma.query_tap(query).to_table().to_pandas()
        else:
            from pyvo.dal import TAPService
            # ALMA TAP service initialization
            service = TAPService(self.almaurl + "/tap")
            ret = service.search(query).to_table().to_pandas()

        # Apply filters based on conditions
        if self.only12m:
            ret = ret[ret['antenna_arrays'].str.contains('DV|DA')]

        if self.onlyFDM:
            ret = ret[ret['velocity_resolution'] < 50000]

        return ret

    def get_data_urls(self, almaquery=True):
        """
        Retrieve data URLs and total size for ALMA observations.

        Args:
            almaquery (bool): If True, query data using myAlma. Otherwise, use alternative methods.
        """
        logging.info("Starting ALMA data query...")
        rlist = self.queryALMA(almaquery=almaquery)
        mous_list = np.unique(rlist['member_ous_uid'])

        total_size = 0.0
        url_list = []

        for id, mous in enumerate(mous_list):
            uid_url_table = self.myAlma.get_data_info(mous)

            # Filter ASDM data
            url_size = [
                [url, size] for url, size in zip(
                    uid_url_table['access_url'],
                    uid_url_table['content_length']
                )
                if '.asdm.sdm.tar' in url
            ]

            if url_size:
                asdm_size = np.sum(
                    [float(size) for _, size in url_size]
                ) / 1024 / 1024 / 1024
                url_list.extend(url_size)
                print(f"[{id + 1}/{len(mous_list)}] {asdm_size:.2f} GB")
                total_size += asdm_size
            else:
                print(f"[{id + 1}/{len(mous_list)}] -> skipped (may be SV)")

        # Convert URL list to numpy array
        url_list = np.array(url_list)

        # Store results in class attributes
        self.rlist = rlist
        self.total_size = total_size
        self.url_list = url_list

        logging.info(f"Total data size: {total_size:.2f} GB")

    def wget_f(self, num):
        getLogger().info("%s start", num)
        if self.replaceNAOJ:
            download_url = (self.url_list[num][0]).replace(
                self.almaurl, "https://almascience.nao.ac.jp")
        else:
            download_url = self.url_list[num][0]
        print('wget -q --no-check-certificate -P ' +
              self.download_d+' '+download_url)
        os.system('wget -q --no-check-certificate -P ' +
                  self.download_d+' '+download_url)
        getLogger().info("%s end", num)

    def init_logger(self):
        handler = StreamHandler()
        handler.setLevel(INFO)
        handler.setFormatter(
            Formatter("[%(asctime)s] [%(threadName)s] %(message)s"))
        logger = getLogger()
        logger.addHandler(handler)
        logger.setLevel(INFO)

    def download(self):
        nFiles = self.url_list.shape[0]
        self.init_logger()
        getLogger().info("main start")
        with ThreadPoolExecutor(
                max_workers=min(nFiles, 5),
                thread_name_prefix="thread"
        ) as executor:
            for i in range(nFiles):
                executor.submit(self.wget_f, i)
            getLogger().info("submit end")
        getLogger().info("main end")
