import fnmatch
import glob
import logging
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import subprocess
import numpy as np

from ._logmgr import (
    initialize_log_listener,
    get_logger_for_subprocess,
    stop_log_listener,
)
from ._process import (
    ProcessData,
    init_process,
    make_calibration_script,
    calibrate,
    remove_target,
    imaging,
    selfcal_and_imaging,
    export_fits,
    DIRS_NAME_IMAGES,
)
from ._query import query
from ._download import download
from ._analysis import Analysis
from ._utils import parse_selection


def _init_worker(logger_name, queue):
    _ = get_logger_for_subprocess(logger_name, queue)


class Almaqso:
    """
    Public class for ALMAQSO.
    Users can call this class and its methods to use ALMAQSO's functionality.

    You can specify multiple bands and cycles with using `,` or `;` and `~`.
    For instance, `"3,6"` (3 and 6), `"3;6"` (3 and 6), `"3~5"` (3 to 5), `"3,7-9"` (3, 7 to 9).

    Args:
        target (list[str] | str): Target source name. If empty list or empty string is given, all sources is targeted.
        band (list[int] | int | str): Band number to work with. Default is "" (all bands).
        cycle (list[int] | int | str): project name to work with. Default is "" (all cycles).
        work_dir (str, optional): Working directory. Default is './'.
        casapath (str, optional): Path to the CASA executable. Default is 'casa'.
    """

    def __init__(
        self,
        target: list[str] | str = "",
        band: list[int] | int | str = "",
        cycle: str = "",
        work_dir: str = "./",
        casapath: str = "casa",
    ) -> None:
        self._band: list[int] = parse_selection(band)
        self._cycle: list[int] = parse_selection(cycle)
        self._work_dir: Path = Path(work_dir).absolute()
        self._original_dir: str = os.getcwd()
        self._casapath: Path = Path(casapath)
        self._log_file_path: Path | None = None
        self._sources: list[str] = list(np.unique(target).astype(str))

        # Prepare working dir
        os.makedirs(self._work_dir, exist_ok=True)

        # Initialize log listener
        self._logger_name, self._log_queue, self._log_listener = (
            initialize_log_listener(self._work_dir)
        )
        logger = logging.getLogger(self._logger_name)
        logger.info("ALMAQSO started.")

    def __enter__(self) -> "Almaqso":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        """Close the log listener and clean up resources."""
        if self._log_listener is not None:
            logger = logging.getLogger(self._logger_name)
            logger.info("ALMAQSO finished.")
            stop_log_listener(self._log_listener)
            self._log_listener = None

    def __del__(self) -> None:
        """Failsafe cleanup when object is garbage collected."""
        self.close()

    def __getstate__(self):
        """Exclude unpicklable objects when serializing for multiprocessing."""
        state = self.__dict__.copy()
        # Remove unpicklable log queue and listener
        state["_log_queue"] = None
        state["_log_listener"] = None
        return state

    def __setstate__(self, state):
        """Restore state after deserialization."""
        self.__dict__.update(state)

    def _pre_process(self) -> None:
        os.chdir(self._work_dir)

        # Remove all *.asdm.sdm.tar
        for file in os.listdir("."):
            if file.endswith(".asdm.sdm.tar"):
                os.remove(file)

    def _post_process(self) -> None:
        logger = logging.getLogger(self._logger_name)
        logger.info("Processing complete.")
        os.chdir(self._original_dir)

    def process(
        self,
        n_parallel: int = 1,
        skip_previous_successful: bool = False,
        do_tclean: bool = False,
        tclean_mode: list[str] = ["mfs"],
        tclean_weightings: tuple[str, str] = ("natural", ""),
        do_selfcal: bool = False,
        kw_selfcal: dict[str, object] = {},
        do_export_fits: bool = False,
        remove_casa_images: bool = False,
        remove_asdm: bool = False,
        remove_intermediate: bool = False,
    ) -> None:
        """
        Download and process ALMA data.

        Args:
            n_parallel (int): The number of the parallel execution.
            do_tclean (bool): Perform tclean. Default is False.
            skip_previous_successful (bool): Skip processing for previously successful tasks. Default is False.
            tclean_mode (list[str]): List of imaging specmodes for tclean. "mfs" creates a MFS image, "mfs_spw" creates MFS images for each spw, and "cube" creates a cube image. Default is ["mfs"].
            tclean_weightings (tuple[str, str]): Weighting scheme and robust parameter for tclean. Second element is the robust parameter for briggs weighting. Default is ("natural", "").
            do_selfcal (bool): Perform self-calibration. Default is False.
            kw_selfcal (dict[str, object]): Parameters for the self-calibration and `tclean` task.
            do_export_fits (bool): Export the final image to FITS format. Default is False.
            remove_casa_images (bool): Remove the CASA images after processing. This option only works if do_tclean is True. Default is False.
            remove_asdm (bool): Remove the ASDM files after processing. Default is False.
            remove_intermediate (bool): Remove the intermediate files after processing. Log of CASA will be retained. Default is False.

        Returns:
            None
        """
        logger: logging.Logger = get_logger_for_subprocess(
            self._logger_name, self._log_queue
        )
        logger.info(f"Working directory: {self._work_dir}")

        try:
            self._pre_process()
        except Exception as e:
            logger.error(f"ERROR: {e}")
            return

        # Search for ALMA data
        url_list = []

        # Log the processing settings
        logger.info("== Processing settings ==")
        logger.info(f"Target sources: {', '.join(self._sources)}")
        logger.info(f"Number of parallel processes: {n_parallel}")
        logger.info(f"Target band: {self._band}")
        logger.info(f"Target cycle: {self._cycle if self._cycle != '' else 'all'}")
        logger.info(
            "I will skip previously successful projects."
            if skip_previous_successful
            else "I will process all projects."
        )
        if do_tclean:
            logger.info(
                f"tclean will be performed. {', '.join(tclean_mode)} images will be created, and the weighting is \"{tclean_weightings[0]}\" with robust=\"{tclean_weightings[1]}\"."
            )
        else:
            logger.info("tclean will NOT be performed.")
        if do_selfcal:
            logger.warning("Self-calibration is specified but NOT IMPLEMENTED YET.")
        else:
            logger.info("Self-calibration will NOT be performed.")
        if do_export_fits:
            logger.info(f"Export to FITS: Yes")
            logger.info(
                f"Remove CASA images after processing: {'Yes' if remove_casa_images else 'No'}"
            )
        else:
            logger.info(f"Export to FITS: No")
        logger.info(f"Remove ASDM after processing: {'Yes' if remove_asdm else 'No'}")
        logger.info(
            f"Remove intermediate files after processing: {'Yes' if remove_intermediate else 'No'}"
        )

        # for source in self._sources:
        try:
            query_result = query(self._sources, self._band, self._cycle)
        except Exception as e:
            logger.error(f"NETWORK ERROR while quering data: {e}")
            return
        for q in query_result:
            data_size = round(float(q["size_bytes"]) / (1024**3), 2)
            logger.info(f"{q['url']} will be downloaded ({data_size} GB)")
            url_list.append(q["url"])

        if len(url_list) == 0:
            logger.warning("No data found for the specified source.")

        # Skip previously successful tasks
        if skip_previous_successful:
            # Load results file
            try:
                with open(self._work_dir / "processing_successful.txt", "r") as f:
                    lines = f.readlines()
                successful_asdms = [line.strip() for line in lines]
                # Filter url_list
                filtered_url_list = []
                for url in url_list:
                    # url: .../2019.1.00195.L_uid___A002_Xe230a1_X142.asdm.sdm.tar
                    # asdm_name: uid___A002_Xe230a1_X142
                    asdm_name = "uid___" + (url.split("_uid___")[1]).replace(
                        ".asdm.sdm.tar", ""
                    )
                    if asdm_name in successful_asdms:
                        logger.info(
                            f"Skipping {asdm_name} as it was processed successfully before."
                        )
                    else:
                        filtered_url_list.append(url)
                url_list = filtered_url_list
            except FileNotFoundError:
                logger.info(
                    "No previous results file found. All tasks will be processed."
                )
        else:
            # Clear previous results file
            try:
                if (self._work_dir / "processing_successful.txt").exists():
                    os.remove(self._work_dir / "processing_successful.txt")
            except Exception as _:
                pass

        analysis_results = []

        # Download and process each data with parallel processing
        with ProcessPoolExecutor(
            max_workers=n_parallel,
            initializer=_init_worker,
            initargs=(self._logger_name, self._log_queue),
        ) as pool:
            future_to_url = {
                pool.submit(
                    self._process_wrapper,
                    url,
                    do_tclean=do_tclean,
                    tclean_mode=tclean_mode,
                    tclean_weightings=tclean_weightings,
                    do_selfcal=do_selfcal,
                    kw_selfcal=kw_selfcal,
                    do_export_fits=do_export_fits,
                    remove_casa_images=remove_casa_images,
                    remove_asdm=remove_asdm,
                    remove_intermediate=remove_intermediate,
                ): url
                for url in url_list
            }

        for fut in as_completed(future_to_url):
            url = future_to_url[fut]
            try:
                filename, result = fut.result()
            except Exception as e:
                logger.error(f"Task failed for {url}: {e}")
                analysis_results.append((url, False))
            else:
                analysis_results.append((filename, result))

        logger.info("== All tasks completed ==")
        # Output the summary of the processing results
        logger.info("Processing results summary:")
        for f in analysis_results:
            filename, result = f
            logger.info(f"{filename}: {'success' if result else 'failed'}")

        self._post_process()

    def _process_wrapper(self, url: str, **kwargs) -> tuple[str, bool]:
        """
        Wrapper function for download and process.

        Args:
            url (str): URL of the ASDM data for download.
            **kwargs: Keyword arguments for process.

        Returns:
            tuple[str, bool]: ASDM name and the result of the processing.
        """
        filename: str | None = None
        asdmname: str | None = None
        ret: bool = False
        logger = logging.getLogger(self._logger_name)
        try:
            filename = download(url)
            logger.info(f"Downloaded {filename} from {url}")
            asdmname, ret = self._process(filename=filename, **kwargs)
            os.chdir(self._work_dir)
        except Exception as e:
            logger.error(f"ERROR while processing {url}: {e}")

        # if failed, remove ASDM and directory if needed.
        if not ret:
            if filename and kwargs.get("remove_asdm", False):
                try:
                    os.remove(filename)
                except Exception as e:
                    logger.warning(f'Failed to remove "{filename}": {e}')
            if asdmname and kwargs.get("remove_intermediate", False):
                if os.path.exists(asdmname):
                    try:
                        shutil.rmtree(asdmname)
                    except Exception as e:
                        logger.warning(f'Failed to remove directory "{asdmname}": {e}')

        if not asdmname:
            asdmname = url.split("/")[-1]

        # Write successful processing to file
        if ret:
            try:
                with open(self._work_dir / "processing_successful.txt", "a") as f:
                    f.write(f"{asdmname}\n")
            except Exception as e:
                logger.warning(
                    f'Failed to write processing result for "{asdmname}": {e}'
                )

        return asdmname, ret

    def _process(
        self,
        filename: str,
        do_tclean: bool,
        tclean_mode: list[str],
        tclean_weightings: tuple[str, str],
        do_selfcal: bool,
        kw_selfcal: dict[str, object],
        do_export_fits: bool,
        remove_casa_images: bool,
        remove_asdm: bool,
        remove_intermediate: bool,
    ) -> tuple[str, bool]:
        """
        Wrapper function for the analysis process.
        """
        logger = logging.getLogger(self._logger_name)
        logger.info(f"Processing file: {filename}")

        # Determine the working directory
        asdmname = "uid___" + (filename.split("_uid___")[1]).replace(
            ".asdm.sdm.tar", ""
        )

        if os.path.exists(asdmname):
            shutil.rmtree(asdmname)
        os.makedirs(asdmname)

        logger.info(f"Working directory: {asdmname}")
        os.chdir(asdmname)

        # Extract the tar file
        logger.info(f"Extracting {filename}")
        try:
            subprocess.run(["tar", "-xf", f"../{filename}"], check=True)
            logger.info("Extraction completed")
        except subprocess.CalledProcessError as e:
            logger.error(f"ERROR: Failed to extract {filename} (reason: {e})")
            logger.error(f"Stop processing {asdmname}")
            return asdmname, False

        process_data: ProcessData = init_process(filename, self._casapath)

        # Make a CASA script
        try:
            logger.info(f"{asdmname}: Creating a calibration script")
            ret = make_calibration_script(process_data)
            logger.info(f"{asdmname}: Generated calibration script")
            if ret is not None:
                logger.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                logger.warning(f"STDERR ({asdmname}): {ret['stderr']}")
        except Exception as e:
            logger.error(f"ERROR while creating a calibration script: {e}")
            logger.error(f"Stop processing {asdmname}")
            return asdmname, False

        # Calibration
        try:
            logger.info(f"{asdmname}: Starting calibration")
            ret = calibrate(process_data)
            logger.info(f"{asdmname}: Calibration completed")
            if ret is not None:
                logger.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                logger.warning(f"STDERR ({asdmname}): {ret['stderr']}")
        except Exception as e:
            logger.error(f"ERROR while calibration: {e}")
            logger.error(f"Stop processing {asdmname}")
            return asdmname, False

        # Remove target
        try:
            logger.info(f"{asdmname}: Removing target")
            ret = remove_target(process_data)
            logger.info(f"{asdmname}: Target removed")
            if ret is not None:
                logger.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                logger.warning(f"STDERR ({asdmname}): {ret['stderr']}")
        except Exception as e:
            logger.error(f"ERROR while removing target: {e}")
            logger.error(f"Stop processing {asdmname}")
            return asdmname, False

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
                logger.info(f"{asdmname}: Performing imaging")
                for mode in tclean_mode:
                    ret = imaging(process_data, mode, kw_tclean)
                    if ret is not None:
                        logger.info(
                            f"STDOUT ({asdmname}, mode={mode}): {ret['stdout']}"
                        )
                        logger.warning(
                            f"STDERR ({asdmname}, mode={mode}): {ret['stderr']}"
                        )
                logger.info(f"{asdmname}: Imaging completed")
            except Exception as e:
                logger.error(f"ERROR while imaging: {e}")
                logger.error(f"Stop processing {asdmname}")
                return asdmname, False

        # self-calibration
        if do_selfcal:
            kw_selfcal["specmode"] = kw_tclean["specmode"]
            try:
                logger.info(f"{asdmname}: Performing self-calibration")
                ret = selfcal_and_imaging(process_data, kw_selfcal, kw_tclean)
                logger.info(f"{asdmname}: Self-calibration completed")
                if ret is not None:
                    logger.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                    logger.warning(f"STDERR ({asdmname}): {ret['stderr']}")
            except Exception as e:
                logger.error(f"ERROR while self-calibration: {e}")
                logger.error(f"Stop processing {asdmname}")
                return asdmname, False

        # Export to FITS
        if do_export_fits:
            try:
                logger.info(f"{asdmname}: Exporting to FITS")
                ret = export_fits(process_data)
                logger.info(f"{asdmname}: Exported to FITS")
                if ret is not None:
                    logger.info(f"STDOUT ({asdmname}): {ret['stdout']}")
                    logger.warning(f"STDERR ({asdmname}): {ret['stderr']}")
                if remove_casa_images:
                    logger.info(f"{asdmname}: Removing CASA images")
                    shutil.rmtree("dirty")
                    logger.info(f"{asdmname}: CASA images removed")
            except Exception as e:
                logger.error(f"ERROR while exporting to FITS: {e}")
                logger.error(f"Stop processing {asdmname}")
                return asdmname, False

        # Remove ASDM files
        if remove_asdm:
            try:
                logger.info(f"{asdmname}: Removing ASDM files")
                os.remove("../" + filename)
                logger.info(f"{asdmname}: ASDM files removed")
            except Exception as e:
                logger.error(f"ERROR while removing ASDM files: {e}")
                logger.warning("Continue the post-processing")

        # Remove intermediate files
        if remove_intermediate:
            try:
                logger.info(f"{asdmname}: Removed intermediate files")
                keep_dirs: list[str] = DIRS_NAME_IMAGES + [
                    process_data.get_vis_name(),
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
                logger.info(f"{asdmname}: Removing intermediate files")
            except Exception as e:
                logger.error(f"ERROR while removing intermediate files: {e}")
                logger.warning("Continue the post-processing")

        # Check if `SEVERE` error is found
        log_files = glob.glob("*.log")

        found_severe_error = False
        for log_file in log_files:
            with open(log_file, "r") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if "SEVERE" in line:
                    logger.error(
                        f"{asdmname}: SEVERE error is found in {log_file} (line: {i+1})"
                    )
                    found_severe_error = True

        # Processing complete
        logger.info(f"Processing {asdmname} is done.")
        return asdmname, not found_severe_error

    # def analysis(self) -> None:
    #     """
    #     Perform the analysis.
    #     """
    #     logger_name, log_queue, log_listener = initialize_log_listener(self._work_dir)
    #     _WORKER_LOGGER_NAME = logger_name
    #     logger = logging.getLogger(_WORKER_LOGGER_NAME)
    #     os.chdir(self._work_dir)

    #     # Search directories starting with "uid___"
    #     dirs = [
    #         d for d in os.listdir(".") if os.path.isdir(d) and d.startswith("uid___")
    #     ]

    #     # For each directory, execute the analysis
    #     for d in dirs:
    #         logger.info(f"Analyzing {d}")
    #         os.chdir(d)
    #         # Perform the analysis
    #         analysis = Analysis()

    #         # Get the spectrum
    #         try:
    #             analysis.get_spectrum()
    #             logger.info(f"{d}: Spectrum analysis completed")
    #             analysis.plot_spectrum()
    #             logger.info(f"{d}: Spectrum plot created")
    #             analysis.write_spectrum_csv()
    #             logger.info(f"{d}: Write spectrum CSV completed")
    #         except Exception as e:
    #             logger.error(f"ERROR while getting spectrum: {e}")
    #             logger.error(f"Stop analyzing {d}")
    #             os.chdir(self._work_dir)
    #             return

    #         # Calculate the optical depth
    #         # try:
    #         #     analysis.calc_optical_depth()
    #         #     logger.info(f"{d}: Optical depth calculation completed")
    #         #     analysis.plot_optical_depth()
    #         #     logger.info(f"{d}: Optical depth plot created")
    #         # except Exception as e:
    #         #     logger.error(f"ERROR while calculating optical depth: {e}")
    #         #     logger.error(f"Stop analyzing {d}")
    #         #     os.chdir(self._work_dir)
    #         #     return

    #         os.chdir(self._work_dir)
