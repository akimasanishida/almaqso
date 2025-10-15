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
            imagename=f"{{dir}}/{{field}}_spw_{{spw}}_mfs",
            deconvolver="hogbom",
            gridder="standard",
            specmode="mfs",
            spw=str(spw),
            field=str(field),
            weighting=weighting,
            robust=robust,
            cell=str(cell),
            imsize=imsize,
            niter=0,
            pbcor=True,
            interactive=False,
            savemodel="{savemodel}",
        )
