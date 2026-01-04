from pathlib import Path
import subprocess


TEMPLATES_DIR: Path = Path(__file__).parent / "_templates"


def run_casa_script(casa_path: Path | str, script_name: Path | str) -> dict:
    """
    Run a CASA script.

    Args:
        casa_path (Path | str): Path to the CASA executable.
        script_name (Path | str): Name of the CASA script to run.

    Returns:
        dict: Dictionary containing 'stdout' and 'stderr' from the CASA run.
    """
    cmd = [str(casa_path), "--nologger", "--nogui", "-c", str(script_name)]
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    ret = {
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    return ret


def create_script_from_template(
    template_name: str, params: dict, suffix: str = ""
) -> Path:
    """
    Create a CASA script from a template.

    Args:
        template_name (str): Name of the template file.
        params (dict): Parameters to fill in the template.
        suffix (str): Suffix to add to the script file name.

    Returns:
        Path: Path to the created script file.
    """
    # Ensure template_name is only file name
    template_name = Path(template_name).name
    # Load the script template
    template_path: Path = TEMPLATES_DIR / template_name
    try:
        with open(template_path, "r") as f:
            script = f.read()
    except FileNotFoundError as e:
        raise RuntimeError(f"Failed to load template {template_name}: {e}")

    try:
        script_content = script.format(**params)
    except Exception as e:
        raise RuntimeError(f"Failed to format script {template_name}: {e}")

    # Generate script file name
    script_name = template_name.lstrip("_").replace(".py", f"{suffix}.py")
    script_path = Path.cwd() / script_name

    # Create a script file
    try:
        with open(script_path, "w") as f:
            f.write(script_content)
    except IOError as e:
        raise RuntimeError(f"Failed to write script {script_name}: {e}")

    return script_path
