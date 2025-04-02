from almaqso.download_archive import download_archive
from almaqso.analysis import analysis

if __name__ == '__main__':
    # download_archive(7, './test.json')
    analysis('.', '/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/casa', verbose=True, skip=False)
