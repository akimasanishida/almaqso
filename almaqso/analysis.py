import fnmatch
import glob
import os
import shutil
import subprocess


def _run_casa_cmd(
    casa: str, mpicasa: str, n_core: int, cmd: str, verbose: bool
) -> None:
    """
    Run a CASA command.

    Args:
        casa (str): Path to the CASA executable. Provide full path even if it is in the PATH for using MPI CASA.
        mpicasa (str): Path to the MPI CASA executable.
        n_core (int): Number of cores to run with MPI CASA.
        cmd (str): CASA command to run.
        verbose (bool): Print the STDOUT of the CASA commands.

    Returns:
        None
    """
    if mpicasa is not None:
        exe = mpicasa
        options = ["-n", str(n_core), casa, "--nologger", "--nogui", "-c", cmd]
    else:
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


def _calibration(casa_options: map) -> None:
    """
    Run the calibration steps.

    Args:
        casa_options (map): Dictionary containing the CASA options.

    Returns:
        None
    """
    scriptfile = glob.glob("*.scriptForCalibration.py")[0]

    with open(scriptfile, "r") as f:
        syscalcheck = f.readlines().copy()[21]

    scriptfile_part = scriptfile.replace(".py", ".part.py")
    with open(scriptfile_part, "w") as f:
        if (
            syscalcheck.split(":")[1].split("'")[1]
            == "Application of the bandpass and gain cal tables"
        ):
            f.write("mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]" + "\n")
        else:
            f.write("mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]" + "\n")
        f.write("applyonly = True" + "\n")
        f.write(f'execfile("{scriptfile}", globals())\n')

    _run_casa_cmd(cmd=f"execfile('{scriptfile_part}', globals())", **casa_options)


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
    mpicasa: bool = False,
    n_core: int = 2,
    skip: bool = True,
    verbose: bool = False,
    remove_others: bool = False,
) -> None:
    """
    Run the analysis of the QSO data.

    Args:
        tardir (str): Directory containing the `*.asdm.sdm.tar` files.
        casapath (str): Path to the CASA executable. Provide full path even if it is in the PATH for using MPI CASA.
        mpicasa (bool): Use MPI CASA. Default is False.
        n_core (int): Number of cores to use for the analysis. Default is 8.
        skip (bool): Skip the analysis if the output directory exists. Default is True.
        verbose (bool): Print the STDOUT of the CASA commands when no errors occur. Default is False.
        remove_others (bool): Remove other files in the output directory. Default is False.

    Returns:
        None
    """
    asdm_files = [
        file for file in os.listdir(f"{tardir}") if file.endswith(".asdm.sdm.tar")
    ]
    almaqso_dir = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]

    if mpicasa:
        mpicasa_path = os.path.dirname(casapath) + "/mpicasa"
    else:
        mpicasa_path = None

    casa_options = {
        "casa": casapath,
        "mpicasa": mpicasa_path,
        "n_core": n_core,
        "verbose": verbose,
    }

    severe_error_list = []

    for asdm_file in asdm_files:
        asdmname = "uid___" + (asdm_file.split("_uid___")[1]).replace(
            ".asdm.sdm.tar", ""
        )
        print(f"Processing {asdmname}")

        if os.path.exists(asdmname) and skip:
            print(f"{asdmname}: analysis already done and skip")
            continue
        if os.path.exists(asdmname):
            print(f"{asdmname}: analysis already done but reanalyzed")
        else:
            os.makedirs(asdmname)

        os.chdir(asdmname)

        os.system(f"tar -xf ../{asdm_file}")

        # Create calibration script
        cmd = (
            f"sys.path.append('{almaqso_dir}');"
            + "from almaqso._qsoanalysis import _make_script;"
            + f"_make_script('{asdm_file}')"
        )
        _run_casa_cmd(cmd=cmd, **casa_options)

        # Calibration
        _calibration(casa_options)

        # Remove target
        cmd = (
            f"sys.path.append('{almaqso_dir}');"
            + "from almaqso._qsoanalysis import _remove_target;"
            + f"_remove_target(parallel={mpicasa})"
        )
        _run_casa_cmd(cmd=cmd, **casa_options)

        if remove_others:
            keep_dirs = {"dirty_cube", "selfcal"}
            for file in os.listdir("."):
                if os.path.isdir(file):
                    if file in keep_dirs:
                        continue
                    if fnmatch.fnmatch(file, "*.ms.split.split"):
                        continue
                    shutil.rmtree(file)
                else:
                    if fnmatch.fnmatch(file, "*.listbos") or fnmatch.fnmatch(file, "*.log"):
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
