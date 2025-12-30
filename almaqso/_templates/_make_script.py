import os
import analysisUtils as aU
import almaqa2csg as csg


try:
    print(f"analysisUtils of {{aU.version()}} will be used.")
except Exception:
    raise Exception("analysisUtils is not found")

# Step 1. Import the ASDM file
kw_importasdm = {{
    "asdm": '{asdm}',
    "vis": '{vis}',
    "asis": "Antenna Station Receiver Source CalAtmosphere CalWVR CorrelatorMode SBSummary",
    "bdfflags": True,
    "lazy": True,
    "flagbackup": False,
}}

importasdm(**kw_importasdm)

# Step 2. Generate a calibration script
if not os.path.exists('./{vis}.scriptForCalibration.py'):
    refant = aU.commonAntennas('{vis}')
    csg.generateReducScript(
        msNames='{vis}',
        refant=refant[0],
        corrAntPos=False,
        useCalibratorService=False,
        useLocalAlmaHelper=False,
    )
