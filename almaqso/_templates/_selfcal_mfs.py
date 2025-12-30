import os
import analysisUtils as aU

vis = "{vis}"
dir = "{dir}"
weighting = "{weighting}"
robust = {robust}

sfsdr = aU.stuffForScienceDataReduction()
fields = aU.getFields(vis)
fields_target = aU.getTargetsForIntent(vis)
fields_cal = list(set(fields) - set(fields_target))

for field in fields_cal:
    caltable = os.path.join(dir, f"phase_{{field}}.cal")
    gaincal(
        vis=vis,
        caltable=caltable,
        field=str(field),
        solint="inf",
        calmode="p",
        refant=sfsdr.getRefAntenna(vis, minDays=""),
        gaintype="G",
    )
    if not os.path.exists(caltable):
        continue
    applycal(
        vis=vis,
        field=str(field),
        gaintable=[caltable],
        interp="linear",
    )
    vis_new = os.path.join(dir, f"{{field}}_selfcal.ms")
    split(
        vis=vis,
        outputvis=vis_new,
        field=str(field),
        datacolumn="corrected",
    )

    cell, imsize, _ = aU.pickCellSize(vis_new, imsize=True, cellstring=True)

    imagename = os.path.join(dir, f"{{field}}_mfs")
    tclean(
        vis=vis_new,
        imagename=imagename,
        deconvolver="hogbom",
        gridder="standard",
        specmode="mfs",
        field=str(field),
        weighting=weighting,
        robust=robust,
        cell=cell,
        imsize=imsize,
        niter=0,
        pbcor=True,
        interactive=False,
    )
