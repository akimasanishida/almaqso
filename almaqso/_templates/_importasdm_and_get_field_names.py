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

# Step 2. Write all field names to temporary text file
if msmd.open('{vis}'):
    field_names = msmd.fieldnames()
    msmd.close()
    with open('./{vis}_field_names.temp', 'w') as f:
        for name in field_names:
            f.write(f"{{name}}\n")
