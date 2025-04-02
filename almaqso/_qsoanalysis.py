import os
import shutil
import glob
from casatasks import *
from casatools import msmetadata
import analysisUtils as aU
import almaqa2csg as csg
from casampi.MPICommandClient import MPICommandClient


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

    casalog.post('almaqso: Generated calibration script is done.')


def _remove_target(parallel) -> None:
    """
    Remove the target fields from the measurement set.

    Args:
        parallel (bool): Running in MPICASA or not.

    Returns:
        None
    """
    if parallel:
        client = MPICommandClient()
        client.set_log_mode('redirect')
        client.start_services()

    visname = glob.glob('*.ms.split')[0]
    print(visname)
    fields = aU.getFields(visname)
    fields_target = aU.getTargetsForIntent(visname)
    fields_cal = list(set(fields) - set(fields_target))
    print(fields_cal)

    kw_split = {
        'vis': visname,
        'outputvis': visname + '.split',
        'field': ', '.join(fields_cal),
        'datacolumn': 'all',
    }

    if parallel:
        command = f'mstransform("{kw_split["vis"]}", "{kw_split["outputvis"]}",' + \
                f'field="{kw_split["field"]}", datacolumn="{kw_split["datacolumn"]}")'
        client.push_command_request(command,block,target_server,parameters)
    else:
        mstransform(**kw_split)

    listobs(vis=kw_split['outputvis'], listfile=kw_split['outputvis'] + '.listobs')


def _create_dirty_image(parallel) -> None:
    """
    Create dirty image with the measurement set by using tclean.

    Args:
        parallel (bool): Running in MPICASA or not.

    Returns:
        None
    """
    visname = glob.glob('*.ms.split.split')[0]
    cell, imsize, _ = aU.pickCellSize(visname, imsize=True, cellstring=True)
    fields = aU.getFields(visname)
    msmd = msmetadata()

    # Create directory
    dir_name = 'dirty_cube'
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    kw_tclean = {
        'vis': visname,
        'specmode': 'cube',
        'veltype': 'radio',
        'nchan': -1,
        'outframe': 'lsrk',
        'cell': cell,
        'imsize': imsize,
        'deconvolver': 'hogbom',
        'weighting': 'natural',
        # 'weighting': 'briggs',
        # 'robust': 0.5,
        'gridder': 'standard',
        'restoringbeam': 'common',
        'niter': 0,
        'interactive': False,
        'pbcor': True,
    }

    if parallel:
        kw_tclean['parallel'] = True

    for field in fields:
        msmd.open(visname)
        spws = msmd.spwsforfield(field)
        msmd.close()
        print('spws:', spws)
        for spw in spws:
            kw_tclean['spw'] = str(spw)
            kw_tclean['imagename'] = f'{dir_name}/{field}_spw{spw}'
            kw_tclean['field'] = str(field)
            tclean(**kw_tclean)

