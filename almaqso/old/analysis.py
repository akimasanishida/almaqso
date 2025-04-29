import fnmatch
import glob
import os
import shutil
import subprocess


def _run_casa_cmd(
    casa: str, cmd: str, verbose: bool
) -> None:
    """
    Run a CASA command.

    Args:
        casa (str): Path to the CASA executable. Provide full path even if it is in the PATH for using MPI CASA.
        cmd (str): CASA command to run.
        verbose (bool): Print the STDOUT of the CASA commands.

    Returns:
        None
    """
    exe = casa
    options = ["--nologger", "--nogui", "-c", cmd]
    try:
        result = subprocess.run(
            [exe] + options,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if verbose:
            print(f"STDOUT for {cmd}:", result.stdout)
            print(f"STDERR for {cmd}:", result.stderr)
    except subprocess.CalledProcessError as e:
        if verbose:
            print(f"Error while executing {cmd}:")
            print(f"Return Code: {e.returncode}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
        raise RuntimeError(f"Error while executing {cmd}: {e.stderr}") from e


def _check_severe_error() -> bool:
    """
    Check the severe error in the log files.

    Args:
        None

    Returns:
        bool: True if the severe error is found, False otherwise
    """
    log_files = glob.glob("*.log")
    result = False

    for log_file in log_files:
        with open(log_file, "r") as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            if "SEVERE" in line:
                print(f"SEVERE error is found in {log_file} (line: {i+1})")
                result = True

    return result


def analysis(
    tardir: str,
    casapath: str,
    skip: bool = True,
    verbose: bool = False,
    tclean_specmode: str = "mfs",
    tclean_weighting: str = "natural",
    tclean_robust: float | int = 0.5,
    tclean_selfcal: bool = False,
    export_fits: bool = False,
    remove_asdm: bool = False,
    remove_others: bool = False,
) -> None:
    """
    Run the analysis of the QSO data.

    Args:
        tardir (str): Directory containing the `*.asdm.sdm.tar` files.
        casapath (str): Path to the CASA executable. Provide full path even if it is in the PATH for using MPI CASA.
        skip (bool): Skip the analysis if the output directory exists. Default is True.
        verbose (bool): Print the STDOUT of the CASA commands when no errors occur. Default is False.
        tclean_specmode (str): Spectral mode for the tclean task. Default is 'mfs'.
        tclean_weighting (str): Weighting for the tclean task. Default is 'natural'.
        tclean_robust (float | int): Robust parameter for the tclean task. Default is 0.5.
        tclean_selfcal (bool): Perform self-calibration. Default is False.
        export_fits (bool): Export the final image to FITS format. Default is False.
        remove_asdm (bool): Remove the ASDM files after processing. Default is False.
        remove_others (bool): Remove other files in the output directory. Default is False.

    Returns:
        None
    """
    casa_options = {
        "casa": casapath,
        "verbose": verbose,
    }

    severe_error_list = []

    for asdm_file in asdm_files:
        # Create dirty cube
        cmd = (
            f"sys.path.append('{almaqso_dir}');"
            + "from almaqso._qsoanalysis import _create_dirty_image;"
            + f"_create_dirty_image(specmode='{tclean_specmode}', weighting='{tclean_weighting}',"
            + f"robust={tclean_robust}, selfcal={tclean_selfcal}, parallel={mpicasa})"
        )
        _run_casa_cmd(cmd=cmd, **casa_options)

        # Export fits
        if export_fits:
            cmd = (
                f"sys.path.append('{almaqso_dir}');"
                + "from almaqso._qsoanalysis import _export_fits;"
                + f"_export_fits()"
            )
            _run_casa_cmd(cmd=cmd, **casa_options)

        # Remove files
        if remove_asdm:
            os.remove(f"../{asdm_file}")
        if remove_others:
            keep_dirs = {"dirty", "dirty_fits", "selfcal", "selfcal_fits"}
            for file in os.listdir("."):
                if os.path.isdir(file):
                    if file in keep_dirs:
                        continue
                    if fnmatch.fnmatch(file, "*.ms.split.split"):
                        continue
                    shutil.rmtree(file)
                else:
                    if fnmatch.fnmatch(file, "*.listobs") or fnmatch.fnmatch(file, "*.log"):
                        continue
                    os.remove(file)

        # Check severe error
        if _check_severe_error():
            severe_error_list.append(asdmname)

        os.chdir("..")
        print(f"Processing {asdmname} is done.")

    print("#" * 80)
    print("All processing is done.")

    if len(severe_error_list) > 0:
        print("The following data have SEVERE errors:")
        for severe_error in severe_error_list:
            print(severe_error)
    else:
        print("No severe errors are found.")
