import sys
import numpy as np
import json
from almaqso._QSOquery import _QSOquery


def download_archive(band: int, jfilename: str):
    """
    Download the archive data from ALMA Science Archive.

    Args:
        band (int): Band number to download.
        jfilename (str): JSON file name.
    """
    try:
        with open(jfilename, 'r') as f:
            jdict = json.load(f)
    except FileNotFoundError:
        print(f'ERROR: File "{jfilename}" not found')
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f'Error: Failed to parse JSON file "{jfilename}" (reason: {e}).')
        sys.exit(1)

    cals = []
    for i in range(len(jdict)):
        cals.append(jdict[i]['names'][0]['name'])

    cals = np.unique(cals)
    print(f'Shape of cals: {cals.shape}')

    for i, sname in enumerate(cals):
        try:
            print(f'[{i+1}/{cals.shape[0]}] {sname} -> start')
            obj = _QSOquery(
                sname,
                band=band,
                replaceNAOJ=True,
                only12m=True,
                onlyFDM=True
            )
            obj.get_data_urls(almaquery=False)
            # np.save(f'./urls/{sname}.B{band}.npy', obj.url_list)
            obj.download()
            print(f'Totla size({sname} B{band}): {obj.total_size} GB')
        except Exception as e:
            print(f'ERROR: {sname} -> failed (reason: {e}) and skipped')
