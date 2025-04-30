import os
import subprocess
from glob import glob
from pathlib import Path
from typing import Dict


class Analysis:
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

    def _create_script_from_template(self, template_name: str, params: dict) -> str:
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

        script_name = os.path.basename(template_name.lstrip("_"))

        # Create a temporary file for the script
        try:
            with open(script_name, "w") as f:
                f.write(script_content)
        except IOError as e:
            raise RuntimeError(f"Failed to write script {script_name}: {e}")

        return script_name

    def _run_casa_script(self, script_name: str) -> Dict[str, str]:
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

    def make_script(self) -> Dict[str, str]:
        """
        Wrapper function for making a CASA script for the QSO analysis.

        Args:
            None

        Returns:
            Dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        try:
            script_name = self._create_script_from_template(
                "_make_script.py",
                {
                    "asdm": self._asdm_path,
                    "vis": self._vis_name,
                },
            )
            ret = self._run_casa_script(script_name)
        except RuntimeError as e:
            raise e

        return ret

    def calibrate(self) -> Dict[str, str]:
        """
        Run the calibration steps.

        Args:
            None

        Returns:
            Dict[str, str]: STDOUT and STDERR of the CASA command.
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

        try:
            self._run_casa_script(scriptfile_part)
            self._vis_name += ".split"
        except RuntimeError as e:
            raise e

    def remove_target(self) -> Dict[str, str]:
        """
        Wrapper function for removing the target from the measurement set.

        Args:
            None

        Returns:
            Dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        try:
            script_name = self._create_script_from_template(
                "_remove_target.py",
                {
                    "vis": self._vis_name,
                },
            )
            ret = self._run_casa_script(script_name)
            self._vis_name += ".split"
        except RuntimeError as e:
            raise e

        return ret

    def tclean(self, kw_tclean: dict) -> Dict[str, str]:
        """
        Wrapper function for tclean.

        Args:
            kw_tclean (dict): Keyword arguments for tclean.

        Returns:
            Dict[str, str]: STDOUT and STDERR of the CASA command.
        """
        # --- １）specmode チェック & params 作成 ---
        specmode = kw_tclean.get("specmode")
        if specmode not in ("cube", "mfs"):
            raise ValueError(f"specmode {specmode!r} is not supported.")

        # 共通パラメータ
        params = {
            "vis":       self._vis_name,
            "dir":       kw_tclean.get("dir", "dirty"),
            "weighting": kw_tclean.get("weighting") or "natural",
            "robust":    kw_tclean.get("robust",  0.5),
            "savemodel": kw_tclean.get("savemodel") or "none",
        }

        # specmode ごとの追加処理
        if specmode == "cube":
            kw_tclean["restoringbeam"] = "common"
            template_name = "_tclean_cube.py"
        else:  # specmode == "mfs"
            kw_tclean["veltype"] = "radio"
            template_name = "_tclean_mfs.py"

        # --- ３）スクリプト生成＆実行 ---
        script_name = self._create_script_from_template(template_name, params)
        try:
            ret = self._run_casa_script(script_name)
        except RuntimeError as e:
            raise e

        return ret
