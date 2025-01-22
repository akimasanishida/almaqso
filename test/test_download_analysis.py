import os
import sys
from pathlib import Path
sys.path.append('../almaqso')  # isort:skip
from almaqso import download_archive, analysis


CURRENT_DIR = Path(__file__).parent
CASA_PATH = '/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8-py3.8.el8/bin/casa'  # Path to CASA. Please modify this if necessary.
DATA_PATH = './uid___A002_Xd68367_X9885'
TARFILE = './2018.1.01575.S_uid___A002_Xd68367_X9885.asdm.sdm.tar'

# Edit the following constants.
DOWNLOAD = False  # True: Download the tar file, False: Use the existing tar file


def test_download():
    if DOWNLOAD:
        os.system(f'rm -rf {TARFILE}')
        download_archive(4, 'catalog/test_2.json')
        assert os.path.exists(TARFILE)


def test_analysis():
    os.system(f'rm -r {DATA_PATH}')

    analysis('.', casacmd=CASA_PATH)
