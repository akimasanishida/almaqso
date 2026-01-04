import analysisUtils as aU

vis = "{vis}"
dir = "{dir}"
weighting = "{weighting}"
robust = float({robust})

cell, imsize, _ = aU.pickCellSize(vis, imsize=True, cellstring=True)
fields = aU.getFields(vis)

for field in fields:
    msmd.open(vis)
    spws = msmd.spwsforfield(field)
    msmd.close()
    for spw in spws:
        tclean(
            vis=vis,
            imagename=f"{{dir}}/{{field}}_spw{{spw}}_mfs",
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
