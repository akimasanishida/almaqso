import os
import subprocess


def _run_casa_cmd(casa, cmd):
    try:
        result = subprocess.run(
            [casa, '--nologger', '--nogui', '-c', cmd],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"STDOUT for {cmd}:", result.stdout)
        print(f"STDERR for {cmd}:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error while executing {cmd}:")
        print(f"Return Code: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise


def analysis(tardir: str, casacmd='casa', skip=True):
    """
    Run the analysis of the QSO data.

    Args:
        tardir (str): Directory containing the `*.asdm.sdm.tar` files.
        casacmd (str): CASA command. Default is 'casa'.
        skip (bool): Skip the analysis if the output directory exists. Default is True.
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
            _run_casa_cmd(casacmd, cmd)

            os.chdir('..')
