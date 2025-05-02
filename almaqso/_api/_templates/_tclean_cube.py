import analysisUtils as aU

vis = "{vis}"
dir = "{dir}"
weighting = "{weighting}"
robust = float({robust})

cell, imsize, _ = aU.pickCellSize(vis, imsize=True, cellstring=True)
fields = aU.getFields(vis)
fields_target = aU.getTargetsForIntent(vis)
fields_cal = list(set(fields) - set(fields_target))

for field in fields_cal:
    msmd.open(vis)
    spws = msmd.spwsforfield(field)
    msmd.close()
    for spw in spws:
        tclean(
            vis=vis,
            imagename=f"{{dir}}/{{field}}_spw{{spw}}_cube",
            deconvolver="hogbom",
            gridder="standard",
            specmode="cube",
            spw=str(spw),
            field=str(field),
            nchan=-1,
            outframe="lsrk",
            veltype="radio",
            weighting=weighting,
            robust=robust,
            cell=str(cell),
            imsize=imsize,
            niter=0,
            pbcor=True,
            interactive=False,
            savemodel="{savemodel}",
        )
