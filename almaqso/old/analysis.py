import fnmatch
import glob
import os
import shutil
import subprocess


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
    remove_others: bool = False,
) -> None:
    severe_error_list = []

    for asdm_file in asdm_files:
        # Remove files
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
