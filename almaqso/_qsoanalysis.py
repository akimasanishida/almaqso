import os
import shutil
import glob
from casatasks import *
import analysisUtils as aU
import almaqa2csg as csg


def _make_script(tarfilename: str) -> None:
    """
    Make a CASA script for the QSO analysis.

    Args:
        tarfilename (str): File name of ASDM tar data.

    Returns:
        None
    """
    try:
        print(f'analysisUtils of {aU.version()} will be used.')
    except Exception:
        raise Exception('analysisUtils is not found')

    # Step 1. Import the ASDM file
    projID = tarfilename.split('_uid___')[0]
    asdmfile = glob.glob(
            f'{os.getcwd()}/{os.path.basename(projID)}/*/*/*/raw/*'
            )[0]
    visname = (os.path.basename(asdmfile)).replace('.asdm.sdm', '.ms')

    kw_importasdm = {
        'asdm': asdmfile,
        'vis': visname,
        'asis': 'Antenna Station Receiver Source CalAtmosphere CalWVR CorrelatorMode SBSummary',
        'bdfflags': True,
        'lazy': True,
        'flagbackup': False,
    }

    shutil.rmtree(kw_importasdm['vis'], ignore_errors=True)
    importasdm(**kw_importasdm)

    casalog.post('almaqso: Import the ASDM file is done.')

    # Step 2. Generate a calibration script
    if not os.path.exists(
            f'./{visname}.scriptForCalibration.py'
            ):
        casalog.post('almaqso: Generate a calibration script.')
        refant = aU.commonAntennas(visname)
        csg.generateReducScript(
            msNames=visname,
            refant=refant[0],
            corrAntPos=False,
            useCalibratorService=False,
            useLocalAlmaHelper=False,
        )

    casalog.post('almaqso: Generated calibration script.')


def _remove_target() -> None:
    """
    Remove the target fields from the measurement set.

    Args:
        None

    Returns:
        None
    """
    visname = glob.glob('*.ms')[0]
    fields = aU.getFields(visname)
    fields_target = aU.getTargetsForIntent(visname)
    fields_cal = list(set(fields) - set(fields_target))

    shutil.rmtree(visname + '.org', ignore_errors=True)
    shutil.move(visname, visname + '.org')

    kw_split = {
        'vis': visname + '.org',
        'outputvis': visname,
        'field': ', '.join(fields_cal),
        'datacolumn': 'all',
    }

    shutil.rmtree(kw_split['outputvis'], ignore_errors=True)
    shutil.rmtree(kw_split['outputvis'] + '.flagversions', ignore_errors=True)

    mstransform(**kw_split)
