import glob
import os
import shutil
from datetime import datetime, timedelta

import almaqa2csg as csg
import analysisUtils as aU
import numpy as np
from casatasks import *
from casatools import msmetadata, table


def _tclean(kw_tclean: dict, fields: list, dir_dirty: str) -> None:
    """
    tclean wrapper for the dirty image.

    Args:
        kw_tclean (dict): Keyword arguments for tclean.
        fields (list): List of fields.
        dir_dirty (str): Directory for dirty images.
    """
    msmd = msmetadata()

    if kw_tclean["specmode"] == "cube":
        kw_tclean["veltype"] = "radio"
        kw_tclean["nchan"] = -1
        kw_tclean["outframe"] = "lsrk"
        kw_tclean["restoringbeam"] = "common"

        for field in fields:
            msmd.open(kw_tclean["vis"])
            spws = msmd.spwsforfield(field)
            msmd.close()
            for spw in spws:
                kw_tclean["spw"] = str(spw)
                kw_tclean["field"] = str(field)
                kw_tclean["imagename"] = f"{dir_dirty}/{field}_spw{spw}_cube"
                tclean(**kw_tclean)
    elif kw_tclean["specmode"] == "mfs":
        for field in fields:
            kw_tclean["field"] = str(field)
            kw_tclean["imagename"] = f"{dir_dirty}/{field}_mfs"
            tclean(**kw_tclean)
    else:
        raise ValueError(f"specmode {specmode} is not supported.")


def _create_dirty_image(specmode, weighting, robust, selfcal) -> None:
    """
    Create dirty image with the measurement set by using tclean.

    Args:

    Returns:
        None
    """
    msfiles = glob.glob("*.ms.split.split")
    if len(msfiles) == 0:
        raise FileNotFoundError("No measurement set found.")
    elif len(msfiles) > 1:
        raise FileExistsError("Multiple measurement sets found.")
    else:
        visname = msfiles[0]

    # Create directory
    dir_dirty = "dirty"
    dir_selfcal = "selfcal"
    if not os.path.exists(dir_dirty):
        os.mkdir(dir_dirty)

    cell, imsize, _ = aU.pickCellSize(visname, imsize=True, cellstring=True)
    fields = aU.getFields(visname)

    kw_tclean = {
        "vis": visname,
        "cell": cell,
        "specmode": specmode,
        "imsize": imsize,
        "deconvolver": "hogbom",
        "weighting": weighting,
        'robust': robust,
        "gridder": "standard",
        "niter": 0,
        "interactive": False,
        "pbcor": True,
    }

    if selfcal:
        kw_tclean["savemodel"] = "modelcolumn"

    _tclean(kw_tclean, fields, dir_dirty)

    # Self-calibration
    sfsdr = aU.stuffForScienceDataReduction()
    if selfcal:
        if not os.path.exists(dir_selfcal):
            os.mkdir(dir_selfcal)
        for field in fields:
            caltable = f"{dir_selfcal}/phase_{field}.cal"
            gaincal(
                vis=visname,
                caltable=caltable,
                field=str(field),
                solint="inf",
                calmode="p",
                refant=sfsdr.getRefAntenna(visname, minDays=""),
                gaintype="G",
            )
            applycal(
                vis=visname,
                field=str(field),
                gaintable=caltable,
                interp="linear",
            )
        split(
            vis=visname,
            outputvis=visname.replace(".ms.split.split", "_selfcal.ms.split.split"),
            datacolumn="corrected",
        )

        kw_tclean["vis"] = visname.replace(".ms.split.split", "_selfcal.ms.split.split")
        kw_tclean["savemodel"] = ""
        _tclean(kw_tclean, fields, dir_selfcal)


def _export_fits():
    """
    Export the images to FITS format.

    Args:
        None

    Returns:
        None
    """
    if os.path.exists("selfcal"):
        input_dir = "selfcal"
    elif os.path.exists("dirty"):
        input_dir = "dirty"
    else:
        raise FileNotFoundError("No image dir found.")        

    output_dir = input_dir + "_fits"

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # Export FITS from all `*.image.pbcor` files in the input directory
    for image_file in glob.glob(f"{input_dir}/*.image.pbcor"):
        # Get the base name of the image file (without path and extension)
        base_name = os.path.basename(image_file).replace(".image.pbcor", "")
        # Construct the output FITS file name
        fits_file = os.path.join(output_dir, f"{base_name}.fits")
        # Export to FITS format
        exportfits(imagename=image_file, fitsimage=fits_file)
