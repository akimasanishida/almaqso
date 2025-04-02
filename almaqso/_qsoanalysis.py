import glob
import os
import shutil
from datetime import datetime, timedelta

import almaqa2csg as csg
import analysisUtils as aU
import numpy as np
from casampi.MPICommandClient import MPICommandClient
from casatasks import *
from casatools import msmetadata, table


def _make_script(tarfilename: str) -> None:
    """
    Make a CASA script for the QSO analysis.

    Args:
        tarfilename (str): File name of ASDM tar data.

    Returns:
        None
    """
    try:
        print(f"analysisUtils of {aU.version()} will be used.")
    except Exception:
        raise Exception("analysisUtils is not found")

    # Step 1. Import the ASDM file
    projID = tarfilename.split("_uid___")[0]
    asdmfile = glob.glob(f"{os.getcwd()}/{os.path.basename(projID)}/*/*/*/raw/*")[0]
    visname = (os.path.basename(asdmfile)).replace(".asdm.sdm", ".ms")

    kw_importasdm = {
        "asdm": asdmfile,
        "vis": visname,
        "asis": "Antenna Station Receiver Source CalAtmosphere CalWVR CorrelatorMode SBSummary",
        "bdfflags": True,
        "lazy": True,
        "flagbackup": False,
    }

    shutil.rmtree(kw_importasdm["vis"], ignore_errors=True)
    importasdm(**kw_importasdm)

    casalog.post("almaqso: Import the ASDM file is done.")

    # Step 2. Generate a calibration script
    if not os.path.exists(f"./{visname}.scriptForCalibration.py"):
        casalog.post("almaqso: Generate a calibration script.")
        refant = aU.commonAntennas(visname)
        csg.generateReducScript(
            msNames=visname,
            refant=refant[0],
            corrAntPos=False,
            useCalibratorService=False,
            useLocalAlmaHelper=False,
        )

    casalog.post("almaqso: Generated calibration script is done.")


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
        client.set_log_mode("redirect")
        client.start_services()

    visname = glob.glob("*.ms.split")[0]
    print(visname)
    fields = aU.getFields(visname)
    fields_target = aU.getTargetsForIntent(visname)
    fields_cal = list(set(fields) - set(fields_target))
    print(fields_cal)

    kw_split = {
        "vis": visname,
        "outputvis": visname + ".split",
        "field": ", ".join(fields_cal),
        "datacolumn": "all",
    }

    if parallel:
        command = (
            f'mstransform("{kw_split["vis"]}", "{kw_split["outputvis"]}",'
            + f'field="{kw_split["field"]}", datacolumn="{kw_split["datacolumn"]}")'
        )
        client.push_command_request(command, block, target_server, parameters)
    else:
        mstransform(**kw_split)

    listobs(vis=kw_split["outputvis"], listfile=kw_split["outputvis"] + ".listobs")


def _mjd_to_datetime(mjd_sec):
    """MJD秒をdatetimeに変換"""
    mjd_days = mjd_sec / 86400.0
    return datetime(1858, 11, 17) + timedelta(days=mjd_days)


def _datetime_to_hhmmss(dt):
    """datetimeをhh:mm:ss形式に変換"""
    return dt.strftime('%H:%M:%S')


def _generate_timerange_halves(vis, field, spw):
    tb = table()
    tb.open(vis)
    query = f"FIELD_ID=={field} && DATA_DESC_ID=={spw}"
    subtb = tb.query(query)
    times = subtb.getcol('TIME')  # MJD秒
    tb.close()

    t_min = np.min(times)
    t_max = np.max(times)
    t_mid = (t_min + t_max) / 2

    dt_min = _mjd_to_datetime(t_min)
    dt_mid = _mjd_to_datetime(t_mid)
    dt_max = _mjd_to_datetime(t_max)

    # 開始日（dt_minの日付）で timerange を作る
    date_str_min = dt_min.strftime('%Y/%m/%d')
    date_str_mid = dt_mid.strftime('%Y/%m/%d')
    date_str_max = dt_max.strftime('%Y/%m/%d')

    timerange1 = f"{date_str_min}/{_datetime_to_hhmmss(dt_min)}~{date_str_mid}/{_datetime_to_hhmmss(dt_mid)}"
    timerange2 = f"{date_str_mid}/{_datetime_to_hhmmss(dt_mid)}~{date_str_max}/{_datetime_to_hhmmss(dt_max)}"

    return timerange1, timerange2


def _create_dirty_image(weighting, robust, split_half, parallel) -> None:
    """
    Create dirty image with the measurement set by using tclean.

    Args:
        parallel (bool): Running in MPICASA or not.

    Returns:
        None
    """
    msmd = msmetadata()
    visname = glob.glob("*.ms.split.split")[0]

    # Create directory
    dir_name = "dirty_cube"
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    cell, imsize, _ = aU.pickCellSize(visname, imsize=True, cellstring=True)
    fields = aU.getFields(visname)

    kw_tclean = {
        "vis": visname,
        "specmode": "cube",
        "veltype": "radio",
        "nchan": -1,
        "outframe": "lsrk",
        "cell": cell,
        "imsize": imsize,
        "deconvolver": "hogbom",
        "weighting": weighting,
        'robust': robust,
        "gridder": "standard",
        "restoringbeam": "common",
        "niter": 0,
        "interactive": False,
        "pbcor": True,
    }

    if parallel:
        kw_tclean["parallel"] = True

    # enumerate for all fields
    for field_id, field in enumerate(fields):
        msmd.open(visname)
        spws = msmd.spwsforfield(field)
        msmd.close()
        print("spws:", spws)
        for spw in spws:
            kw_tclean["spw"] = str(spw)
            kw_tclean["field"] = str(field)
            if split_half:
                timerange1, timerange2 = _generate_timerange_halves(visname, field_id, spw)
                kw_tclean["imagename"] = f"{dir_name}/{field}_spw{spw}_1st_half"
                kw_tclean["timerange"] = timerange1
                print(timerange1)
                tclean(**kw_tclean)
                kw_tclean["imagename"] = f"{dir_name}/{field}_spw{spw}_2nd_half"
                kw_tclean["timerange"] = timerange2
                print(timerange2)
                tclean(**kw_tclean)
            else:
                kw_tclean["imagename"] = f"{dir_name}/{field}_spw{spw}"
                tclean(**kw_tclean)
