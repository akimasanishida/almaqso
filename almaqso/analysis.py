import os
import subprocess


def _run_casa_cmd(casa, mpicasa, n_core, cmd, verbose):
    try:
        result = subprocess.run(
            [mpicasa, '-n', n_core, casa, '--nologger', '--nogui', '-c', cmd],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if verbose:
            print(f"STDOUT for {cmd}:", result.stdout)
        print(f"STDERR for {cmd}:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error while executing {cmd}:")
        print(f"Return Code: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise


def analysis(tardir: str, casapath, mpicasa='mpicasa', n_core=8, skip=True, verbose=False) -> None:
    """
    Run the analysis of the QSO data.

    Args:
        tardir (str): Directory containing the `*.asdm.sdm.tar` files.
        casapath (str): Path to the CASA executable.
        mpicasa (str): Path to the MPI CASA executable. Default is 'mpicasa'.
        n_core (int): Number of cores to use for the analysis. Default is 8.
        skip (bool): Skip the analysis if the output directory exists. Default is True.
        verbose (bool): Print the STDOUT of the CASA commands. Default is False.

    Returns:
        None
    """
    asdm_files = [file for file in os.listdir(f'{tardir}') if file.endswith('.asdm.sdm.tar')]
    almaqso_dir = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]

    for asdm_file in asdm_files:
        asdmname = 'uid___' + (asdm_file.split('_uid___')[1]).replace('.asdm.sdm.tar', '')
        print(f'Processing {asdmname}')
        if os.path.exists(asdmname) and skip:
            print(f'{asdmname}: analysis already done and skip')
        else:
            if os.path.exists(asdmname):
                print(f'asdmname: analysis already done but reanalyzed')

            os.makedirs(asdmname)
            os.chdir(asdmname)
            os.system(f'tar -xf ../{asdm_file}')

            cmd = f"sys.path.append('{almaqso_dir}');" + \
                "from almaqso._qsoanalysis import _qsoanalysis;" + \
                f"_qsoanalysis('{asdm_file}', '{casacmd}')"
            _run_casa_cmd(casacmd, cmd, verbose)

            os.chdir('..')
