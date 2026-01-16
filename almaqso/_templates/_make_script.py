import os
import analysisUtils as aU
import almaqa2csg as csg


if not os.path.exists('./{vis}.scriptForCalibration.py'):
    refant = aU.commonAntennas('{vis}')
    csg.generateReducScript(
        msNames='{vis}',
        refant=refant[0],
        corrAntPos=False,
        useCalibratorService=False,
        useLocalAlmaHelper=False,
    )
