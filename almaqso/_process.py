import os
import shutil
from glob import glob
from pathlib import Path
from dataclasses import dataclass
from typing import Any
from ._casa_runner import run_casa_script, create_script_from_template


DIR_NAME_TCLEAN: str = "dirty"
DIR_NAME_SELFCAL: str = "selfcal"
DIRS_NAME_IMAGES = [
    DIR_NAME_TCLEAN,
    DIR_NAME_SELFCAL,
    DIR_NAME_TCLEAN + "_fits",
    DIR_NAME_SELFCAL + "_fits",
]


@dataclass
class ProcessData:
    _project_id: str
    _asdm_path: Path
    _vis_name: str
    _casapath: Path

    def get_vis_name(self) -> str:
        """
        Get the measurement set name.

        Returns:
            str: Measurement set name.
        """
        return self._vis_name


def init_process(tarfile_name: str, casapath: Path) -> ProcessData:
    """
    Initialize the ProcessData.

    Args:
        tarfile_name (str): Name of `*asdm.sdm.tar` file.
        casapath (str): Path to the CASA executable.

    Returns:
        ProcessData: Initialized ProcessData object.
    """
    _project_id = tarfile_name.split("_uid___")[0]
    _asdm_path = Path(glob(f"{_project_id}/*/*/*/raw/*")[0])
    _vis_name = _asdm_path.name.replace(".asdm.sdm", ".ms")
    _casapath = casapath

    return ProcessData(
        _project_id=_project_id,
        _asdm_path=_asdm_path,
        _vis_name=str(_vis_name),
        _casapath=_casapath,
    )


def make_calibration_script(process_data: ProcessData) -> dict[str, str]:
    """
    Wrapper function for making a CASA script for the calibration.

    Args:
        process_data (ProcessData): ProcessData object.

    Returns:
        dict[str, str]: STDOUT and STDERR of the CASA command.
    """
    script_name = create_script_from_template(
        "_make_script.py",
        {
            "asdm": process_data._asdm_path,
            "vis": process_data._vis_name,
        },
    )
    ret = run_casa_script(process_data._casapath, script_name)

    return ret


def calibrate(process_data: ProcessData) -> dict[str, str]:
    """
    Run the calibration steps.

    Args:
        process_data (ProcessData): ProcessData object.

    Returns:
        dict[str, str]: STDOUT and STDERR of the CASA command.
    """
    scriptfile = f"{process_data._vis_name}.scriptForCalibration.py"

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
                f.write("mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]" + "\n")
            else:
                f.write(
                    "mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]" + "\n"
                )
            f.write("applyonly = True" + "\n")
            f.write(f'execfile("{scriptfile}", globals())\n')
    except IOError as e:
        raise RuntimeError(f"Failed to write script {scriptfile_part}: {e}")

    ret = run_casa_script(process_data._casapath, scriptfile_part)
    process_data._vis_name += ".split"

    return ret


def remove_target(process_data: ProcessData) -> dict[str, str]:
    """
    Remove the target from the measurement set.

    Args:
        process_data (ProcessData): ProcessData object.

    Returns:
        dict[str, str]: STDOUT and STDERR of the CASA command.
    """
    script_name = create_script_from_template(
        "_remove_target.py",
        {
            "vis": process_data._vis_name,
        },
    )
    ret = run_casa_script(process_data._casapath, script_name)
    process_data._vis_name += ".split"

    return ret


def imaging(
    process_data: ProcessData, mode: str, kw_tclean: dict[str, Any]
) -> dict[str, str]:
    """
    Create dirty images.

    Args:
        process_data (ProcessData): ProcessData object.
        mode (str): Imaging mode.
        kw_tclean (dict[str, Any]): Keyword arguments for tclean.

    Returns:
        dict[str, str]: STDOUT and STDERR of the CASA command.
    """
    kw_tclean["vis"] = process_data._vis_name
    kw_tclean["dir"] = DIR_NAME_TCLEAN

    # set specmode
    if mode == "mfs":
        template_name = "_tclean_mfs.py"
    elif mode == "mfs_spw":
        template_name = "_tclean_mfs_spw.py"
    elif mode == "cube":
        template_name = "_tclean_cube.py"
    else:
        raise ValueError(f"mode {mode!r} is not supported.")

    script_name = create_script_from_template(template_name, kw_tclean)
    ret = run_casa_script(process_data._casapath, script_name)

    return ret


def selfcal_and_imaging(
    process_data: ProcessData, kw_selfcal: dict[str, Any], kw_tclean: dict[str, Any]
) -> dict[str, str]:
    """
    Run self-calibration and create second dirty images.

    Args:
        process_data (ProcessData): ProcessData object.
        kw_selfcal (dict[str, Any]): Keyword arguments for self-calibration.
        kw_tclean (dict[str, Any]): Keyword arguments for tclean.

    Returns:
        dict[str, str]: STDOUT and STDERR of the CASA command.
    """
    # --- １）specmode チェック & params 作成 ---
    specmode = kw_selfcal.get("specmode")
    if specmode not in ("cube", "mfs"):
        raise ValueError(f"specmode {specmode!r} is not supported.")

    if os.path.exists(DIR_NAME_SELFCAL):
        shutil.rmtree(DIR_NAME_SELFCAL)
    os.makedirs(DIR_NAME_SELFCAL)

    # 共通パラメータ
    params = {
        "vis": process_data._vis_name,
        "dir": DIR_NAME_SELFCAL,
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
    script_name = create_script_from_template(template_name, params)
    ret = run_casa_script(process_data._casapath, script_name)

    return ret


def export_fits(process_data: ProcessData) -> dict[str, str]:
    """
    Export images to FITS format.

    Args:
        process_data (ProcessData): ProcessData object.

    Returns:
        dict[str, str]: STDOUT and STDERR of the CASA command.
    """
    ret = {
        "stdout": "",
        "stderr": "",
    }

    # dirty images
    if os.path.exists(DIR_NAME_TCLEAN):
        script_name = create_script_from_template(
            "_export_fits.py",
            {
                "dir": DIR_NAME_TCLEAN,
            },
            "_dirty",
        )
        ret_dirty = run_casa_script(process_data._casapath, script_name)
        ret["stdout"] += ret_dirty["stdout"] + "\n"
        ret["stderr"] += ret_dirty["stderr"] + "\n"

    # selfcal images
    if os.path.exists(DIR_NAME_SELFCAL):
        script_name = create_script_from_template(
            "_export_fits.py",
            {
                "dir": DIR_NAME_SELFCAL,
            },
            "_selfcal",
        )
        ret_selfcal = run_casa_script(process_data._casapath, script_name)
        ret["stdout"] += ret_selfcal["stdout"] + "\n"
        ret["stderr"] += ret_selfcal["stderr"] + "\n"

    return ret


def get_image_dirs() -> list[str]:
    """
    Get the list of image directories.

    Args:
        None

    Returns:
        list[str]: List of image directories.
    """
    return [
        DIR_NAME_TCLEAN,
        DIR_NAME_SELFCAL,
        DIR_NAME_TCLEAN + "_fits",
        DIR_NAME_SELFCAL + "_fits",
    ]
