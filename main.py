from almaqso.analysis import analysis
from almaqso.download_archive import download_archive

if __name__ == '__main__':
    # download_archive(7, './catalog/test_2.json')
    analysis(
        ".",
        "/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/casa",
        verbose=True,
        skip=False,
        remove_others=True,
    )
