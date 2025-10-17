import os
import shutil
import subprocess
from glob import glob
from pathlib import Path
import csv
import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from spectral_cube import SpectralCube


class Process:
    def __init__(self, tarfile_name: str, casapath: str) -> None:
        """
        Initialize the Analysis class.

        Args:
            tarfile_name (str): Name of `*asdm.sdm.tar` file.
            casapath (str): Path to the CASA executable.

        Returns:
            None
        """
        self._tarfile_name = tarfile_name
        self._project_id = tarfile_name.split("_uid___")[0]
        self._asdm_path = glob(f"{self._project_id}/*/*/*/raw/*")[0]
        self._vis_name = (os.path.basename(self._asdm_path)).replace(".asdm.sdm", ".ms")
        self._casapath = casapath
        self._templates_dir = os.path.join(os.path.dirname(__file__), "_templates")
    def    self._dir_tclean = "dirty"
        self._dir_selfcal = "selfcal"
        self._dir_plot = "plots"

    def _create_script_from_template(
        self, template_name: str, params: dict, suffix: str = ""
    ) -> str:
        """
        Generate script file from template
        """
        # Load the script template
        template_path = os.path.join(self._templates_dir, template_name)
        try:
            with open(template_path, "r") as f:
                script = f.read()
        except FileNotFoundError as e:
            raise RuntimeError(f"Failed to load template {template_name}: {e}")

        try:
            script_content = script.format(**params)
        except Exception as e:
            raise RuntimeError(f"Failed to format script {template_name}: {e}")

        script_name = os.path.basename(template_name.lstrip("_")) + suffix

        # Create a temporary file for the script
        try:
            with open(script_name, "w") as f:
                f.write(script_content)
        except IOError as e:
            raise RuntimeError(f"Failed to write script {script_name}: {e}")

        return script_name

    def _run_casa_script(self, script_name: str) -> dict[str, str]:
        """
        Run a CASA script.
        """
        # Run the CASA script
        cmd = [self._casapath, "--nologger", "--nogui", "-c", script_name]
        try:
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            ret = {
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
            return ret
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"stdout: {e.stdout}, stderr:{e.stderr}") from e

    def make_script(self) -> dict[str, str]:
        """
        Wrapper function for making a CASA script for the QSO analysis.

        Args:
            None

        Returns:
            dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        script_name = self._create_script_from_template(
            "_make_script.py",
            {
                "asdm": self._asdm_path,
                "vis": self._vis_name,
            },
        )
        ret = self._run_casa_script(script_name)

        return ret

    def calibrate(self) -> dict[str, str]:
        """
        Run the calibration steps.

        Args:
            None

        Returns:
            dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        scriptfile = f"{self._vis_name}.scriptForCalibration.py"

        try:
            with open(scriptfile, "r") as f:
                syscalcheck = f.readlines().copy()[21]
        except FileNotFoundError as e:
            raise RuntimeError(f"Failed to load script {scriptfile}: {e}")

        scriptfile_part = scriptfile.replace(".py", ".part.py")
        try:
            with open(scriptfile_part, "w") as f:
                if (
                    syscalcheck.split(":")[1].split("'")[1]
                    == "Application of the bandpass and gain cal tables"
                ):
                    f.write(
                        "mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]" + "\n"
                    )
                else:
                    f.write(
                        "mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]" + "\n"
                    )
                f.write("applyonly = True" + "\n")
                f.write(f'execfile("{scriptfile}", globals())\n')
        except IOError as e:
            raise RuntimeError(f"Failed to write script {scriptfile_part}: {e}")

        ret = self._run_casa_script(scriptfile_part)
        self._vis_name += ".split"

        return ret

    def remove_target(self) -> dict[str, str]:
        """
        Wrapper function for removing the target from the measurement set.

        Args:
            None

        Returns:
            dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        script_name = self._create_script_from_template(
            "_remove_target.py",
            {
                "vis": self._vis_name,
            },
        )
        ret = self._run_casa_script(script_name)
        self._vis_name += ".split"

        return ret

    def tclean(self, mode: str, kw_tclean: dict) -> dict[str, str]:
        """
        Wrapper function for tclean.

        Args:
            kw_tclean (dict): Keyword arguments for tclean.

        Returns:
            dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        kw_tclean["vis"] = self._vis_name
        kw_tclean["dir"] = self._dir_tclean

        # set specmode
        if mode == "mfs":
            template_name = "_tclean_mfs.py"
        elif mode == "mfs_spw":
            template_name = "_tclean_mfs_spw.py"
        elif mode == "cube":
            template_name = "_tclean_cube.py"
        else:
            raise ValueError(f"mode {mode!r} is not supported.")

        script_name = self._create_script_from_template(template_name, kw_tclean)
        ret = self._run_casa_script(script_name)

        return ret

    def selfcal(self, kw_selfcal: dict) -> dict[str, str]:
        """
        Wrapper function for self-calibration.

        Args:
            kw_selfcal (dict): Keyword arguments for self-calibration.

        Returns:
            dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        # --- １）specmode チェック & params 作成 ---
        specmode = kw_selfcal.get("specmode")
        if specmode not in ("cube", "mfs"):
            raise ValueError(f"specmode {specmode!r} is not supported.")

        if os.path.exists(self._dir_selfcal):
            shutil.rmtree(self._dir_selfcal)
        os.makedirs(self._dir_selfcal)

        # 共通パラメータ
        params = {
            "vis": self._vis_name,
            "dir": self._dir_selfcal,
            "weighting": kw_selfcal.get("weighting") or "natural",
            "robust": kw_selfcal.get("robust", 0.5),
        }

        # specmode ごとの追加処理
        if specmode == "cube":
            kw_selfcal["restoringbeam"] = "common"
            template_name = "_selfcal_cube.py"
        else:
            template_name = "_selfcal_mfs.py"

        # --- ３）スクリプト生成＆実行 ---
        script_name = self._create_script_from_template(template_name, params)
        ret = self._run_casa_script(script_name)

        return ret

    def export_fits(self) -> dict[str, str]:
        """
        Wrapper function for exporting images to FITS format.

        Args:
            None

        Returns:
            dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        template_name = "_export_fits.py"
        ret = {
            "stdout": "",
            "stderr": "",
        }

        # dirty images
        if os.path.exists(self._dir_tclean):
            script_name = self._create_script_from_template(
                template_name,
                {
                    "dir": self._dir_tclean,
                },
                "_dirty",
            )
            ret_dirty = self._run_casa_script(script_name)
            ret["stdout"] += ret_dirty["stdout"] + "\n"
            ret["stderr"] += ret_dirty["stderr"] + "\n"

        # selfcal images
        if os.path.exists(self._dir_selfcal):
            script_name = self._create_script_from_template(
                template_name,
                {
                    "dir": self._dir_selfcal,
                },
                "_selfcal",
            )
            ret_selfcal = self._run_casa_script(script_name)
            ret["stdout"] += ret_selfcal["stdout"] + "\n"
            ret["stderr"] += ret_selfcal["stderr"] + "\n"

        return ret

    # def plot_spectrum(self) -> dict[str, str]:
    #     """
    #     Plot the spectrum of the target from the cube FITS image.
    #     """
    #     ret = {
    #         "stdout": "",
    #         "stderr": "",
    #     }

    #     # Search "dirty_fits/*.fits" files
    #     fits_files = glob("dirty_fits/*.fits")

    #     for fits_file in fits_files:
    #         with fits.open(fits_file) as hdul:
    #             # Get the beam size in pixels
    #             data_header = hdul[0].header
    #             CDELT = abs(data_header["CDELT1"])
    #             cell_size = CDELT * 3600  # arcsec
    #             beam_size = hdul[1].data[0][0]  # arcsec
    #             beam_px = round(beam_size / cell_size)  # px
    #             # print(f"Cell size: {cell_size} arcsec")
    #             # print(f"Beam size: {beam_size} arcsec")
    #             # print(f"Beam size in pixels: {beam_px} px")
    #             BMAJ = hdul[1].data[0][0]  # arcsec
    #             BMIN = hdul[1].data[0][1]  # arcsec

    #             # Get the center pixel of the image
    #             x_center = data_header["CRPIX1"]
    #             y_center = data_header["CRPIX2"]

    #             # Extract the region around the peak of the image with the beam size
    #             data_extract = hdul[0].data[
    #                 0,
    #                 :,
    #                 int(y_center - beam_px / 2) : int(y_center + beam_px / 2),
    #                 int(x_center - beam_px / 2) : int(x_center + beam_px / 2),
    #             ]

    #             # Calculate the total flux density
    #             beam_area = (np.pi / (4 * np.log(2))) * BMAJ * BMIN
    #             total_flux_intensity = (
    #                 np.sum(data_extract, axis=(1, 2)) * cell_size**2 / beam_area
    #             )

    #             # Frequency axis
    #             CRVAL3 = data_header["CRVAL3"]
    #             CRPIX3 = data_header["CRPIX3"]
    #             CDELT3 = data_header["CDELT3"]

    #             freqs = (
    #                 CRVAL3 + (np.arange(data_extract.shape[0]) - (CRPIX3 - 1)) * CDELT3
    #             ) / 1e9  # in GHz

    #             # Create the output directory
    #             os.makedirs(self._dir_plot, exist_ok=True)

    #             # Output the spectrum to a text file (*.csv)
    #             fits_name = os.path.basename(fits_file)
    #             output_file = os.path.join(self._dir_plot, f"{fits_name}_spectrum.csv")

    #             with open(output_file, "w", newline="") as csvfile:
    #                 writer = csv.writer(csvfile)
    #                 writer.writerow(["Frequency", "Integrated flux"])  # ヘッダー
    #                 for f, i in zip(freqs, total_flux_intensity):
    #                     writer.writerow([f, i])


    #             # Calculate y-axis limits based on the standard deviation
    #             y_mean = np.mean(total_flux_intensity)
    #             y_std = np.std(total_flux_intensity)
    #             y_min = y_mean - 5 * y_std
    #             y_max = y_mean + 5 * y_std

    #             # Get the minimun larger than y_min and maximum smaller than y_max
    #             y_min_data = np.min(total_flux_intensity[total_flux_intensity > y_min])
    #             y_max_data = np.max(total_flux_intensity[total_flux_intensity < y_max])

    #             y_min_lim = y_mean - (y_mean - y_min_data) * 1.2
    #             y_max_lim = y_mean + (y_max_data - y_mean) * 1.2

    #             # Plot the spectrum
    #             fig, ax = plt.subplots()
    #             ax.plot(freqs, total_flux_intensity)
    #             ax.set_xlabel("Frequency (GHz)")
    #             ax.set_ylabel("Integrated flux (Jy)")
    #             ax.set_ylim(y_min_lim, y_max_lim)
    #             ax.set_title(f"Spectrum from {fits_name}")
    #             ax.grid()
    #             fig.tight_layout()
    #             fig.savefig(os.path.join(self._dir_plot, f"{fits_name}_spectrum.png"), dpi=300)

    #     return ret

    def get_image_dirs(self) -> list[str]:
        """
        Get the list of image directories.

        Args:
            None

        Returns:
            list[str]: List of image directories.
        """
        return [
            self._dir_tclean,
            self._dir_selfcal,
            self._dir_tclean + "_fits",
            self._dir_selfcal + "_fits",
        ]

    def get_vis_name(self) -> str:
        """
        Get the name of the visibility file.

        Args:
            None

        Returns:
            str: Name of the visibility file.
        """
        return self._vis_name
