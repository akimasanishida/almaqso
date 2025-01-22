import os
import shutil
import numpy as np
import glob
from itertools import chain
from casatasks import importasdm, mstransform
import analysisUtils as aU
import almaqa2csg as csg


# def _writelog(content: str):
#     os.system('touch almaqso_analysis.log')
#     os.system(f'echo "{content}" >> almaqso_analysis.log')


def _qsoanalysis(tarfilename, casacmd):
    projID = tarfilename.split('_uid___')[0]

    try:
        print(f'analysisUtils of {aU.version()} will be used.')
    except Exception:
        raise Exception('analysisUtils is not found')

    # Step 1. Import the ASDM file
    asdmfile = glob.glob(f'{os.getcwd()}/{os.path.basename(projID)}/*/*/*/raw/*')[0]
    visname = (os.path.basename(asdmfile)).replace('.asdm.sdm', '.ms')

    kw_importasdm = {
        'asdm': asdmfile,
        'vis': visname,
        'asis': 'Antenna Station Receiver Source CalAtmosphere CalWVR CorrelatorMode SBSummary',
        'bdfflags': True,
        'lazy': True,
        'flagbackup': False,
    }

    os.system(f'rm -rf {kw_importasdm["vis"]}')
    importasdm(**kw_importasdm)

    casalog.post('Step 1. Import the ASDM file is done.')

    # Step 2. Generate a calibration script
    try:
        refant = aU.commonAntennas(visname)
        vis = visname
    except Exception:
        refant = aU.commonAntennas(visname + '.split')
        vis = visname + '.split'

    if os.path.exists(
            f'./log/{visname}.scriptForCalibration.py'
        ):
        # print('Calibration script already exists.')
        os.system(f'cp ./log/{visname}.scriptForCalibration.py ./')
    else:
        # print('Calibration script will be generated.')
        csg.generateReducScript(
                msNames = vis,
                refant = refant[0],
                corrAntPos = False,
                useCalibratorService = False,
                useLocalAlmaHelper = False
        )
        os.makedirs('./log', exist_ok=True)
        os.system(f'cp {visname}.scriptForCalibration.py ./log/')

    dish_diameter = aU.almaAntennaDiameter(refant[0])

    casalog.post('Step 2. Generate a calibration script is done.')

    # Step 3. Remove the target source
    intentListASDM = aU.getIntentsFromASDM(asdmfile)
    intentList = list(chain.from_iterable(intentListASDM.values()))
    intentList_unique = np.unique(intentList)
    intentList_non_target = intentList_unique[intentList_unique != 'OBSERVE_TARGET']

    shutil.rmtree(visname + '.org', ignore_errors=True)
    shutil.move(visname, visname + '.org')

    kw_mstransform = {
        'vis': visname + '.org',
        'outputvis': visname,
        'datacolumn': 'all',
        'intent': '*,'.join(intentList_non_target) + '*',
        'keepflags': True
    }

    shutil.rmtree(kw_mstransform['outputvis'], ignore_errors=True)
    shutil.rmtree(kw_mstransform['outputvis'] + '.flagversions', ignore_errors=True)

    mstransform(**kw_mstransform)

    casalog.post('Step 3. Remove the target source is done.')

    # Step 4. Calibration
    cmdfile = visname + '.scriptForCalibration.py'

    with open(cmdfile, 'r') as f:
        syscalcheck = f.readlines().copy()[21]

    # checksteps = open(cmdfile, 'r')
    # syscalcheck = checksteps.readlines().copy()[21]
    # checksteps.close()

    # f = open(cmdfile.replace('.py', '.part.py'), 'w')
    if syscalcheck.split(':')[1].split("'")[1] == \
            'Application of the bandpass and gain cal tables':
        # f.write(
        #     'mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]'+'\n')
        mysteps = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    else:
        # f.write(
        #     'mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]'+'\n')
        mysteps = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    # f.write('applyonly = True'+'\n')
    # f.write('execfile('+'"'+cmdfile+'"'+',globals())'+'\n')
    # f.close()
    applyonly = True
    execfile(cmdfile, globals())

    # cmd = '"' + \
    #     'execfile('+"'"+cmdfile.replace('.py',
    #                                     '.part.py')+"'"+')' + '"'
    # os.system(casacmd+' --nologger --nogui -c '+cmd)

    casalog.post('Step 4. Calibration is done.')
