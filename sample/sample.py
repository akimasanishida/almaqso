from almaqso import download_archive, analysis


if __name__ == '__main__':
    download_archive(4, 'catalog/test_2.json')
    analysis('.', casacmd='/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/casa')  # Specify the path to the CASA binary
