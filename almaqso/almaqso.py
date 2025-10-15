import fnmatch
import glob
import json
import logging
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import subprocess

import coloredlogs
import numpy as np
from tqdm import tqdm

from ._api import Process, Query, download
from ._api import Analysis


class Almaqso:
    def __init__(
        self,
        # json_filename: str,
        target: list[str],
        band: int,
        work_dir: str = "./",
        casapath: str = "casa",
    ) -> None:
        """
        Args:
            json_filename (str): JSON file name obtained from the ALMA Calibration Catalog.
            target (str): Target source name.
            band (int): Band number to work with.
            work_dir (str): Working directory. Default is './'.
            casapath (str): Path to the CASA executable. Default is 'casa'.
        """
        self._band: int = band
        self._work_dir: Path = Path(work_dir).absolute()
        self._original_dir: str = os.getcwd()
        self._casapath: str = casapath
        self._log_file_path: Path | None = None
        self._sources: np.ndarray = np.unique(target)

        # Load the JSON file
        # try:
        #     with open(json_filename, "r") as f:
        #         jdict = json.load(f)
        # except FileNotFoundError:
        #     logging.error(f'ERROR: File "{json_filename}" not found')
        #     return
        # except json.JSONDecodeError as e:
        #     logging.error(
        #         f'Error: Failed to parse JSON file "{json_filename}" (reason: {e}).'
        #     )
        #     return

        # Prepare working dir
        os.makedirs(self._work_dir, exist_ok=True)
        self._init_logger()
        logging.info(f"Working directory: {self._work_dir}")

        # Get source names
        # try:
        #     sources_list = [entry["names"][0]["name"] for entry in jdict]
        # except KeyError as e:
        #     logging.error(
        #         f"ERROR: Failed to extract source names from JSON file. (reason: {e})"
        #     )
        #     return
        # self._sources = np.unique(sources_list)

    def _init_logger(self) -> None:
        """
        Initialize the logger
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        if logger.hasHandlers():
            logger.handlers.clear()

        fmt = (
            "[%(asctime)s] [%(threadName)s %(processName)s] [%(levelname)s] %(message)s"
        )
        datefmt = "%H:%M:%S"

        formatter = logging.Formatter(
            fmt,
            datefmt=datefmt,
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # coloredlogs は StreamHandler に影響する
        coloredlogs.install(
            level=logging.INFO,
            logger=logger,
            fmt=fmt,
            datefmt=datefmt,
        )

        # --- FileHandler（ファイル保存、色なし） ---
        if self._log_file_path is None:
            now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_file_path = Path(self._work_dir) / f"almaqso_{now_str}.log"
            logging.info(f"Log file: {self._log_file_path}")
            mode = "w"
        else:
            mode = "a"
        file_handler = logging.FileHandler(
            self._log_file_path, mode=mode, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # tqdmとの相性：必要なら tqdm.write() を使う
        tqdm.write = lambda x, *args, **kwargs: logging.getLogger().info(x)

    def _pre_process(self) -> None:
        os.chdir(self._work_dir)

        # Remove all *.asdm.sdm.tar
        for file in os.listdir("."):
            if file.endswith(".asdm.sdm.tar"):
                os.remove(file)

    def _post_process(self) -> None:
        logging.info("Processing complete.")
        os.chdir(self._original_dir)

    def process(
        self,
        n_parallel: int = 1,
        do_tclean: bool = False,
        tclean_mode: list[str] = ["mfs"],
        tclean_weightings: tuple[str, str] = ("natural", ""),
        do_selfcal: bool = False,
        kw_selfcal: dict[str, object] = {},
        do_export_fits: bool = False,
        remove_asdm: bool = False,
        remove_intermediate: bool = False,
    ) -> None:
        """
        Download and process ALMA data.

        Args:
            n_parallel (int): The number of the parallel execution.
            do_tclean (bool): Perform tclean. Default is False.
            tclean_mode (list[str]): List of imaging specmodes for tclean. "mfs" creates a MFS image, "mfs_spw" creates MFS images for each spw, and "cube" creates a cube image. Default is ["mfs"].
            tclean_weightings (tuple[str, str]): Weighting scheme and robust parameter for tclean. Second element is the robust parameter for briggs weighting. Default is ("natural", "").
            do_selfcal (bool): Perform self-calibration. Default is False.
            kw_selfcal (dict[str, object]): Parameters for the self-calibration and `tclean` task.
            do_export_fits (bool): Export the final image to FITS format. Default is False.
            remove_asdm (bool): Remove the ASDM files after processing. Default is False.
            remove_intermediate (bool): Remove the intermediate files after processing. Log of CASA will be retained. Default is False.

        Returns:
            None
        """
        try:
            self._pre_process()
        except Exception as e:
            logging.error(f"ERROR: {e}")
            return

        # Search for ALMA data
        url_list = []

        for source in self._sources:
            logging.info(f"Target source: {source}")
            try:
                query = Query(source, self._band).query()
            except Exception as e:
                logging.error(f"NETWORK ERROR while quering {source}: {e}")
                return
            for q in query:
                data_size = round(float(q["size_bytes"]) / (1024**3), 2)
                logging.info(f"{q['url']} will be downloaded ({data_size} GB)")
                url_list.append(q["url"])

        if len(url_list) == 0:
            logging.warning("No data found for the specified source.")

        # Download and process each data with parallel processing
        with (
            ProcessPoolExecutor(max_workers=n_parallel) as proc_pool,
            ThreadPoolExecutor(max_workers=2) as dl_pool,
        ):

            def _schedule_analysis(future):
                try:
                    filename = future.result()
                    logging.info(f"Downloaded file: {filename}")
                except Exception as e:
                    logging.error(f"Download task failed: {e}")
                else:
                    proc_pool.submit(
                        self._process,
                        filename,
                        do_tclean,
                        tclean_mode,
                        tclean_weightings,
                        do_selfcal,
                        kw_selfcal,
                        do_export_fits,
                        remove_asdm,
                        remove_intermediate,
                    )

            for url in url_list:
                logging.info(f"Downloading from: {url}")
                fut = dl_pool.submit(download, url)
                fut.add_done_callback(_schedule_analysis)

            dl_pool.shutdown(wait=True)
            proc_pool.shutdown(wait=True)

        logging.info("== All tasks completed ==")

        self._post_process()

    def _process(
        self,
        filename: str,
        do_tclean: bool,
        tclean_mode: list[str],
        tclean_weightings: tuple[str, str],
        do_selfcal: bool,
        kw_selfcal: dict[str, object],
        do_export_fits: bool,
        remove_asdm: bool,
        remove_intermediate: bool,
    ) -> None:
        """
        Wrapper function for the analysis process.
        """
        self._init_logger()
        logging.info(f"Processing file: {filename}")

        # Determine the working directory
        asdmname = "uid___" + (filename.split("_uid___")[1]).replace(
            ".asdm.sdm.tar", ""
        )

        if os.path.exists(asdmname):
            shutil.rmtree(asdmname)
        os.makedirs(asdmname)

        logging.info(f"Working directory: {asdmname}")
        os.chdir(asdmname)

        # Extract the tar file
        logging.info(f"Extracting {filename}")
        try:
            subprocess.run(["tar", "-xf", f"../{filename}"], check=True)
            logging.info("Extraction completed")
        except subprocess.CalledProcessError as e:
            logging.error(f"ERROR: Failed to extract {filename} (reason: {e})")
            logging.error(f"Stop processing {asdmname}")
            return

        analysis = Process(filename, self._casapath)

        # Make a CASA script
        try:
            logging.info(f"{asdmname}: Creating a calibration script")
            ret = analysis.make_script()
            logging.info(f"{asdmname}: Generated calibration script")
            if ret is not None:
                logging.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                logging.warning(f"STDERR ({asdmname}): {ret['stderr']}")
        except Exception as e:
            logging.error(f"ERROR while creating a calibration script: {e}")
            logging.error(f"Stop processing {asdmname}")
            return

        # Calibration
        try:
            logging.info(f"{asdmname}: Starting calibration")
            ret = analysis.calibrate()
            logging.info(f"{asdmname}: Calibration completed")
            if ret is not None:
                logging.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                logging.warning(f"STDERR ({asdmname}): {ret['stderr']}")
        except Exception as e:
            logging.error(f"ERROR while calibration: {e}")
            logging.error(f"Stop processing {asdmname}")
            return

        # Remove target
        try:
            logging.info(f"{asdmname}: Removing target")
            analysis.remove_target()
            logging.info(f"{asdmname}: Target removed")
        except Exception as e:
            logging.error(f"ERROR while removing target: {e}")
            logging.error(f"Stop processing {asdmname}")
            return

        # tclean
        kw_tclean: dict[str, object] = {
            "weighting": tclean_weightings[0],
            "robust": tclean_weightings[1],
        }
        if do_tclean:
            if do_selfcal:
                kw_tclean["savemodel"] = "modelcolumn"
            else:
                kw_tclean["savemodel"] = "none"
            try:
                logging.info(f"{asdmname}: Performing imaging")
                for mode in tclean_mode:
                    analysis.tclean(mode, kw_tclean)
                logging.info(f"{asdmname}: Imaging completed")
            except Exception as e:
                logging.error(f"ERROR while imaging: {e}")
                logging.error(f"Stop processing {asdmname}")
                return

        # self-calibration
        if do_selfcal:
            kw_selfcal["specmode"] = kw_tclean["specmode"]
            try:
                logging.info(f"{asdmname}: Performing self-calibration")
                analysis.selfcal(kw_selfcal)
                logging.info(f"{asdmname}: Self-calibration completed")
            except Exception as e:
                logging.error(f"ERROR while self-calibration: {e}")
                logging.error(f"Stop processing {asdmname}")
                return

        # Export to FITS
        if do_export_fits:
            try:
                logging.info(f"{asdmname}: Exporting to FITS")
                analysis.export_fits()
                logging.info(f"{asdmname}: Exported to FITS")
            except Exception as e:
                logging.error(f"ERROR while exporting to FITS: {e}")
                logging.error(f"Stop processing {asdmname}")
                return

        # Remove ASDM files
        if remove_asdm:
            try:
                logging.info(f"{asdmname}: Removing ASDM files")
                os.remove("../" + filename)
                logging.info(f"{asdmname}: ASDM files removed")
            except Exception as e:
                logging.error(f"ERROR while removing ASDM files: {e}")
                logging.warning("Continue the post-processing")

        # Remove intermediate files
        if remove_intermediate:
            try:
                logging.info(f"{asdmname}: Removed intermediate files")
                keep_dirs: list[str] = analysis.get_image_dirs() + [
                    analysis.get_vis_name()
                ]
                keep_files: list[str] = [
                    "*.py",
                    "*.log",
                ]
                for path in os.listdir("."):
                    if os.path.isdir(path):
                        if path in keep_dirs:
                            continue
                        shutil.rmtree(path)
                    else:
                        if any(
                            fnmatch.fnmatch(path, pattern) for pattern in keep_files
                        ):
                            continue
                        os.remove(path)
                logging.info(f"{asdmname}: Removing intermediate files")
            except Exception as e:
                logging.error(f"ERROR while removing intermediate files: {e}")
                logging.warning("Continue the post-processing")

        # If tclean was performed as `specmode="cube"`
        # and the cube image was exported as FITS,
        # then create a spectrum plot
        # if do_tclean and kw_tclean["specmode"] == "cube" and do_export_fits:
        #     try:
        #         logging.info(f"{asdmname}: Creating spectrum plot")
        #         analysis.plot_spectrum()
        #         logging.info(f"{asdmname}: Spectrum plot created")
        #     except Exception as e:
        #         logging.error(f"ERROR while creating spectrum plot: {e}")
        #         logging.error(f"Stop processing {asdmname}")
        #         return

        # Check if `SEVERE` error is found
        log_files = glob.glob("*.log")

        found_severe_error = False
        for log_file in log_files:
            with open(log_file, "r") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if "SEVERE" in line:
                    logging.error(
                        f"{asdmname}: SEVERE error is found in {log_file} (line: {i+1})"
                    )
                    found_severe_error = True

        if found_severe_error:
            logging.error(f"SEVERE error is found in {asdmname}")
        else:
            logging.info(f"No severe errors are found in {asdmname}")

        # Return to the original directory
        os.chdir(self._work_dir)

        # Processing complete
        logging.info(f"Processing {asdmname} is done.")

    def analysis(self) -> None:
        """
        Perform the analysis.
        """
        os.chdir(self._work_dir)

        # Search directories starting with "uid___"
        dirs = [
            d for d in os.listdir(".") if os.path.isdir(d) and d.startswith("uid___")
        ]

        # For each directory, execute the analysis
        for d in dirs:
            logging.info(f"Analyzing {d}")
            os.chdir(d)
            # Perform the analysis
            analysis = Analysis()

            # Get the spectrum
            try:
                analysis.get_spectrum()
                logging.info(f"{d}: Spectrum analysis completed")
                analysis.plot_spectrum()
                logging.info(f"{d}: Spectrum plot created")
            except Exception as e:
                logging.error(f"ERROR while getting spectrum: {e}")
                logging.error(f"Stop analyzing {d}")
                os.chdir(self._work_dir)
                return

            # Calculate the optical depth
            # try:
            #     analysis.calc_optical_depth()
            #     logging.info(f"{d}: Optical depth calculation completed")
            #     analysis.plot_optical_depth()
            #     logging.info(f"{d}: Optical depth plot created")
            # except Exception as e:
            #     logging.error(f"ERROR while calculating optical depth: {e}")
            #     logging.error(f"Stop analyzing {d}")
            #     os.chdir(self._work_dir)
            #     return

            os.chdir(self._work_dir)
